    // IN2011 Computer Networks
    // Coursework 2024/2025
    //
    // Submission by
    //  Amid Olundegun
    //  220029511
    //  Amid.olundegun@city.ac.uk


    // DO NOT EDIT starts
    // This gives the interface that your code must implement.
    // These descriptions are intended to help you understand how the interface
    // will be used. See the RFC for how the protocol works.

    import java.io.IOException;
    import java.math.BigInteger;
    import java.net.*;
    import java.nio.charset.StandardCharsets;
    import java.security.MessageDigest;
    import java.security.NoSuchAlgorithmException;
    import java.util.*;
    import java.util.concurrent.*;

    interface NodeInterface {

        /* These methods configure your node.
         * They must both be called once after the node has been created but
         * before it is used. */

        // Set the name of the node.
        public void setNodeName(String nodeName) throws Exception;

        // Open a UDP port for sending and receiving messages.
        public void openPort(int portNumber) throws Exception;


        /*
         * These methods query and change how the network is used.
         */

        // Handle all incoming messages.
        // If you wait for more than delay miliseconds and
        // there are no new incoming messages return.
        // If delay is zero then wait for an unlimited amount of time.
        public void handleIncomingMessages(int delay) throws Exception;

        // Determines if a node can be contacted and is responding correctly.
        // Handles any messages that have arrived.
        public boolean isActive(String nodeName) throws Exception;

        // You need to keep a stack of nodes that are used to relay messages.
        // The base of the stack is the first node to be used as a relay.
        // The first node must relay to the second node and so on.

        // Adds a node name to a stack of nodes used to relay all future messages.
        public void pushRelay(String nodeName) throws Exception;

        // Pops the top entry from the stack of nodes used for relaying.
        // No effect if the stack is empty
        public void popRelay() throws Exception;


        /*
         * These methods provide access to the basic functionality of
         * CRN-25 network.
         */

        // Checks if there is an entry in the network with the given key.
        // Handles any messages that have arrived.
        public boolean exists(String key) throws Exception;

        // Reads the entry stored in the network for key.
        // If there is a value, return it.
        // If there isn't a value, return null.
        // Handles any messages that have arrived.
        public String read(String key) throws Exception;

        // Sets key to be value.
        // Returns true if it worked, false if it didn't.
        // Handles any messages that have arrived.
        public boolean write(String key, String value) throws Exception;

        // If key is set to currentValue change it to newValue.
        // Returns true if it worked, false if it didn't.
        // Handles any messages that have arrived.
        public boolean CAS(String key, String currentValue, String newValue) throws Exception;

    }
    // DO NOT EDIT ends

    // Complete this!
    public class Node implements NodeInterface {
        // Node configuration
        private String nodeName;
        private DatagramSocket socket;
        private int port;

        // Storage using ConcurrentHashMap for thread safety
        private final ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, String> addressStore = new ConcurrentHashMap<>();

        // Network communication
        private final ExecutorService executor = Executors.newFixedThreadPool(10);
        private volatile boolean running = true;
        private final BlockingQueue<DatagramPacket> messageQueue = new LinkedBlockingQueue<>();

        // Node discovery
        private final ConcurrentHashMap<String, String> knownNodes = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> lastSeen = new ConcurrentHashMap<>();

        // Relay functionality using Stack
        private final Stack<String> relayStack = new Stack<>();
        private final MessageDigest sha256;

        // Request tracking
        private final ConcurrentHashMap<String, CompletableFuture<String>> pendingRequests = new ConcurrentHashMap<>();

        public Node() throws NoSuchAlgorithmException {
            this.sha256 = MessageDigest.getInstance("SHA-256");
        }

        @Override
        public void setNodeName(String nodeName) throws Exception {
            this.nodeName = nodeName;
            String hashID = computeHashID(nodeName);
            addressStore.put(nodeName, hashID);
        }

        @Override
        public void openPort(int portNumber) throws Exception {
            this.port = portNumber;
            this.socket = new DatagramSocket(portNumber);
            startMessageReceiver();
        }

        private void startMessageReceiver() {
            executor.execute(() -> {
                byte[] buffer = new byte[1024];
                while (running) {
                    try {
                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                        socket.receive(packet);
                        messageQueue.offer(new DatagramPacket(
                                Arrays.copyOf(packet.getData(), packet.getLength()),
                                packet.getLength(),
                                packet.getAddress(),
                                packet.getPort()
                        ));
                    } catch (IOException e) {
                        if (running) System.err.println("Receive error: " + e.getMessage());
                    }
                }
            });
        }

        @Override
        public void handleIncomingMessages(int delay) throws Exception {
            long endTime = delay == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + delay;
            while (System.currentTimeMillis() < endTime) {
                DatagramPacket packet = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (packet != null) processPacket(packet);
            }
        }

        private void processPacket(DatagramPacket packet) {
            String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            String[] parts = message.split(" ", 3);
            if (parts.length < 2) return;

            String txID = parts[0];
            String messageType = parts[1];
            String payload = parts.length > 2 ? parts[2] : "";

            switch (messageType) {
                case "READ": handleReadRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case "WRITE": handleWriteRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case "RELAY": handleRelayRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case "DISCOVER": handleDiscoveryRequest(txID, packet.getAddress(), packet.getPort()); break;
            }
        }

        private void handleReadRequest(String txID, String key, InetAddress address, int port) {
            String hashID = computeHashID(key);
            String value = dataStore.get(hashID);
            String response = txID + " READ_RESPONSE " + (value != null ? value : "NOT_FOUND");
            sendMessage(response, address, port);
        }

        private void handleWriteRequest(String txID, String payload, InetAddress address, int port) {
            String[] parts = payload.split(" ", 2);
            if (parts.length == 2) {
                String key = parts[0];
                String value = parts[1];
                String hashID = computeHashID(key);
                dataStore.put(hashID, value);
                String response = txID + " WRITE_RESPONSE OK";
                sendMessage(response, address, port);

                // Replicate to closest nodes
                replicateWrite(key, value);
            } else {
                String response = txID + " WRITE_RESPONSE ERROR";
                sendMessage(response, address, port);
            }
        }

        private String sendRequestToNearestNodes(String txID, String key) throws Exception {
            String hashID = computeHashID(key);
            String request = txID + " N " + hashID;

            // Send to all known nodes (in real implementation would select some)
            for (Map.Entry<String, String> entry : knownNodes.entrySet()) {
                String[] addrParts = entry.getValue().split(":");
                sendMessage(request, InetAddress.getByName(addrParts[0]), Integer.parseInt(addrParts[1]));
            }

            // Wait for response (simplified - would use proper async handling)
            DatagramPacket responsePacket = waitForResponse(txID);
            return new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
        }

        private List<String> parseNearestNodesResponse(String response) {
            List<String> nodes = new ArrayList<>();
            String[] parts = response.split(" ");
            if (parts.length >= 3 && parts[1].equals("O")) {
                for (int i = 2; i < parts.length; i++) {
                    nodes.add(parts[i]);
                }
            }
            return nodes;
        }

        private String queryNodeForData(String txID, String key, String ip, int port) throws Exception {
            String request = txID + " R " + key;
            sendMessage(request, InetAddress.getByName(ip), port);

            DatagramPacket responsePacket = waitForResponse(txID);
            String response = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
            String[] parts = response.split(" ", 3);

            if (parts.length >= 3 && parts[1].equals("S")) {
                return parts[2]; // Return the value
            }
            return null;
        }

        private String generateTxID() {
            return UUID.randomUUID().toString().substring(0, 8);
        }
        private DatagramPacket waitForResponse(String txID) throws Exception {
            // Simplified - would use proper timeout and matching logic
            byte[] buffer = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            return packet;
        }
        private void handleReadResponse(String txID, String payload, InetAddress address, int port) {
            // Format: txID S value
            String[] parts = payload.split(" ", 2);
            if (parts.length == 2) {
                String value = parts[1];
                // In a real implementation, we would match this to a pending request
                // and complete the future with this value
            }
        }


        private void handleNearestResponse(String txID, String payload, InetAddress address, int port) {
            // Format: txID O node1:port1 node2:port2 node3:port3
            String[] nodes = payload.split(" ");
            List<String> nearestNodes = Arrays.asList(nodes).subList(1, nodes.length);

            // Store these nodes as potential candidates for future requests
            for (String node : nearestNodes) {
                String[] parts = node.split(":");
                if (parts.length == 2) {
                    knownNodes.put(parts[0], node); // Store as "ip:port"
                }
            }

            // In a real implementation, we would match this to a pending request
            // and complete the future associated with this txID
        }

        private void handleRelayRequest(String txID, String payload, InetAddress sender, int senderPort) {
            if (!relayStack.isEmpty()) {
                String nextNode = relayStack.peek();
                String nextAddress = knownNodes.get(nextNode);
                if (nextAddress != null) {
                    try {
                        String[] addrParts = nextAddress.split(":");
                        sendMessage(txID + " RELAY " + payload,
                                InetAddress.getByName(addrParts[0]),
                                Integer.parseInt(addrParts[1]));
                    } catch (Exception e) {
                        System.err.println("Relay failed: " + e.getMessage());
                    }
                }
            }
        }


        private void replicateWrite(String key, String value) {
            String hashID = computeHashID(key);
            List<String> closestNodes = findClosestNodes(hashID, 3);
            for (String node : closestNodes) {
                String address = knownNodes.get(node);
                if (address != null) {
                    try {
                        String[] addrParts = address.split(":");
                        sendMessage("REPLICATE_WRITE " + key + " " + value,
                                InetAddress.getByName(addrParts[0]),
                                Integer.parseInt(addrParts[1]));
                    } catch (Exception e) {
                        System.err.println("Replication failed for node " + node + ": " + e.getMessage());
                    }
                }
            }
        }

        // Relay functionality using Stack
        @Override
        public void pushRelay(String nodeName) throws Exception {
            relayStack.push(nodeName);
        }

        @Override
        public void popRelay() throws Exception {
            if (!relayStack.isEmpty()) {
                relayStack.pop();
            }
        }


        // Data operations
        @Override
        public boolean exists(String key) throws Exception {
            return dataStore.containsKey(computeHashID(key));
        }

        @Override
        public String read(String key) throws Exception {
            // First try local storage
            String localValue = dataStore.get(key);
            if (localValue != null) {
                return localValue;
            }

            // If not found locally, follow CRN protocol to find nearest nodes
            String currentTxID = generateTxID();
            String nearestNodesResponse = sendRequestToNearestNodes(currentTxID, key);

            // Parse the nearest nodes response
            List<String> nearestNodes = parseNearestNodesResponse(nearestNodesResponse);

            // Query each nearest node for the data
            for (String nodeAddress : nearestNodes) {
                String[] parts = nodeAddress.split(":");
                String value = queryNodeForData(currentTxID, key, parts[0], Integer.parseInt(parts[1]));
                if (value != null) {
                    return value;
                }
            }

            return null;
        }

        @Override
        public boolean write(String key, String value) throws Exception {
            String hashID = computeHashID(key);
            dataStore.put(hashID, value);

            // Replicate to closest nodes
            List<String> closestNodes = findClosestNodes(hashID, 3);
            for (String node : closestNodes) {
                String address = knownNodes.get(node);
                if (address != null) {
                    String[] addrParts = address.split(":");
                    sendMessage("WRITE " + key + " " + value,
                            InetAddress.getByName(addrParts[0]),
                            Integer.parseInt(addrParts[1]));
                }
            }
            return true;
        }

        @Override
        public boolean CAS(String key, String currentValue, String newValue) throws Exception {
            String hashID = computeHashID(key);
            if (currentValue.equals(dataStore.get(hashID))) {
                dataStore.put(hashID, newValue);

                // Replicate CAS operation
                List<String> closestNodes = findClosestNodes(hashID, 3);
                for (String node : closestNodes) {
                    String address = knownNodes.get(node);
                    if (address != null) {
                        String[] addrParts = address.split(":");
                        sendMessage("CAS " + key + " " + currentValue + " " + newValue,
                                InetAddress.getByName(addrParts[0]),
                                Integer.parseInt(addrParts[1]));
                    }
                }
                return true;
            }
            return false;
        }

        // Helper methods
        private String computeHashID(String key) {
            byte[] hash = sha256.digest(key.getBytes(StandardCharsets.UTF_8));
            return String.format("%064x", new BigInteger(1, hash));
        }

        private List<String> findClosestNodes(String targetHash, int count) {
            List<Map.Entry<String, String>> nodes = new ArrayList<>(knownNodes.entrySet());
            nodes.sort(Comparator.comparingInt(node ->
                    calculateDistance(targetHash, computeHashID(node.getKey()))));

            List<String> result = new ArrayList<>();
            for (int i = 0; i < Math.min(count, nodes.size()); i++) {
                result.add(nodes.get(i).getKey());
            }
            return result;
        }

        private int calculateDistance(String hash1, String hash2) {
            BigInteger h1 = new BigInteger(hash1, 16);
            BigInteger h2 = new BigInteger(hash2, 16);
            return h1.xor(h2).bitLength();
        }

        private void sendMessage(String message, InetAddress address, int port) {
            try {
                byte[] data = message.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
                socket.send(packet);
            } catch (IOException e) {
                System.err.println("Send failed: " + e.getMessage());
            }
        }

        private String sendRequest(String request, InetAddress address, int port) {
            CompletableFuture<String> future = new CompletableFuture<>();
            String txID = UUID.randomUUID().toString().substring(0, 8);
            pendingRequests.put(txID, future);

            sendMessage(txID + " " + request, address, port);

            try {
                return future.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                return null;
            } finally {
                pendingRequests.remove(txID);
            }
        }

        // Node discovery
        private void handleDiscoveryRequest(String txID, InetAddress sender, int senderPort) {
            String response = txID + " DISCOVER_RESPONSE " + nodeName + " " +
                    socket.getLocalAddress().getHostAddress() + ":" + port;
            sendMessage(response, sender, senderPort);
        }

        @Override
        public boolean isActive(String nodeName) throws Exception {
            String address = knownNodes.get(nodeName);
            if (address == null) return false;

            String[] addrParts = address.split(":");
            String response = sendRequest("PING",
                    InetAddress.getByName(addrParts[0]),
                    Integer.parseInt(addrParts[1]));
            return response != null && response.equals("PONG");
        }

        // Cleanup
        public void shutdown() {
            running = false;
            executor.shutdown();
            if (socket != null) socket.close();
        }
    }