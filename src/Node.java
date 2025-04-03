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
    import java.net.DatagramPacket;
    import java.net.UnknownHostException;
    import java.net.DatagramSocket;
    import java.net.InetAddress;
    import java.nio.charset.StandardCharsets;
    import java.security.MessageDigest;
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

        // Storage
        private final ConcurrentMap<String, String> addressStore = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, String> dataStore = new ConcurrentHashMap<>();
        private final Deque<String> relayStack = new ConcurrentLinkedDeque<>();

        // Network
        private final Random random = new Random();
        private final MessageDigest sha256;
        private final ExecutorService executorService = Executors.newFixedThreadPool(10);
        private final BlockingQueue<DatagramPacket> messageQueue = new LinkedBlockingQueue<>();
        private volatile boolean isRunning;

        // Node discovery
        private final ConcurrentMap<String, String> knownNodes = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Long> lastSeen = new ConcurrentHashMap<>();

        // Request tracking
        private final ConcurrentMap<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
        private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();

        private static class PendingRequest {
            final byte[] message;
            final InetAddress address;
            final int port;
            long expiryTime;
            int retries;

            PendingRequest(byte[] message, InetAddress address, int port, long expiryTime, int retries) {
                this.message = message;
                this.address = address;
                this.port = port;
                this.expiryTime = expiryTime;
                this.retries = retries;
            }
        }

        public Node() throws Exception {
            this.sha256 = MessageDigest.getInstance("SHA-256");
            timeoutExecutor.scheduleAtFixedRate(this::checkTimeouts, 1, 1, TimeUnit.SECONDS);
        }

        @Override
        public void setNodeName(String nodeName) throws Exception {
            this.nodeName = "N:" + nodeName;
            String hashID = computeHashID(this.nodeName);
            addressStore.put(this.nodeName, hashID);
        }

        @Override
        public void openPort(int portNumber) throws Exception {
            if (socket != null) {
                socket.close();
            }
            socket = new DatagramSocket(portNumber);
            isRunning = true;
            executorService.submit(this::receiveMessages);
        }

        private void receiveMessages() {
            byte[] buffer = new byte[1024];
            while (isRunning) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    // Create a new DatagramPacket with the received data and address/port
                    DatagramPacket queuedPacket = new DatagramPacket(
                            Arrays.copyOf(packet.getData(), packet.getLength()),
                            packet.getLength(),
                            packet.getAddress(),
                            packet.getPort()
                    );
                    messageQueue.offer(queuedPacket);
                } catch (IOException e) {
                    if (isRunning) {
                        System.err.println("Error receiving message: " + e.getMessage());
                    }
                }
            }
        }

        private void checkTimeouts() {
            long now = System.currentTimeMillis();
            pendingRequests.entrySet().removeIf(entry -> {
                PendingRequest req = entry.getValue();
                if (now > req.expiryTime) {
                    if (req.retries < 3) {
                        req.retries++;
                        req.expiryTime = now + 5000;
                        sendUDPMessage(req.message, req.address, req.port);
                        return false;
                    }
                    return true;
                }
                return false;
            });
        }

        @Override
        public void handleIncomingMessages(int delay) throws Exception {
            long endTime = delay == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + delay;
            while (System.currentTimeMillis() < endTime) {
                DatagramPacket packet = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (packet == null) continue;
                processIncomingMessage(packet);
            }
        }

        private void processIncomingMessage(DatagramPacket packet) {
            String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            String[] parts = message.split(" ", 3);
            if (parts.length < 2) return;

            byte[] txID = parts[0].getBytes(StandardCharsets.UTF_8);
            char messageType = parts[1].charAt(0);
            String payload = parts.length > 2 ? parts[2] : "";

            switch (messageType) {
                case 'G': handleNameRequest(txID, packet.getAddress(), packet.getPort()); break;
                case 'N': handleNearestRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'E': handleKeyExistenceRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'R': handleReadRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'W': handleWriteRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'C': handleCompareAndSwapRequest(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'V': handleRelayMessage(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'H': handleNameResponse(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'O': handleNearestResponse(txID, payload, packet.getAddress(), packet.getPort()); break;
                case 'S': handleReadResponse(txID, payload, packet.getAddress(), packet.getPort()); break;
            }
        }
//Test
        @Override
        public boolean isActive(String nodeName) throws Exception {
            String nodeAddress = knownNodes.get(nodeName);
            if (nodeAddress == null) return false;

            byte[] txID = generateTransactionID();
            String requestMessage = new String(txID) + " G ";

            String[] addrParts = nodeAddress.split(":");
            sendWithRetries(requestMessage.getBytes(),
                    InetAddress.getByName(addrParts[0]),
                    Integer.parseInt(addrParts[1]));

            return true;
        }

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

        @Override
        public boolean exists(String key) throws Exception {
            String hashID = computeHashID(key);
            return dataStore.containsKey(hashID) || addressStore.containsKey(key);
        }

        @Override
        public String read(String key) throws Exception {
            String hashID = computeHashID(key);
            if (dataStore.containsKey(hashID)) {
                return dataStore.get(hashID);
            }

            // Find nearest nodes if not found locally
            List<String> nearest = findClosestNodes(hashID, 3);
            for (String nodeAddress : nearest) {
                String[] addrParts = nodeAddress.split(":");
                String result = sendReadRequest(key,
                        InetAddress.getByName(addrParts[0]),
                        Integer.parseInt(addrParts[1]));
                if (result != null) {
                    return result;
                }
            }
            return null;
        }

        @Override
        public boolean write(String key, String value) throws Exception {
            String hashID = computeHashID(key);
            dataStore.put(hashID, value);

            List<String> nearest = findClosestNodes(hashID, 3);
            for (String nodeAddress : nearest) {
                String[] addrParts = nodeAddress.split(":");
                sendWriteRequest(key, value,
                        InetAddress.getByName(addrParts[0]),
                        Integer.parseInt(addrParts[1]));
            }
            return true;
        }

        @Override
        public boolean CAS(String key, String currentValue, String newValue) throws Exception {
            String hashID = computeHashID(key);
            if (dataStore.replace(hashID, currentValue, newValue)) {
                List<String> nearest = findClosestNodes(hashID, 3);
                for (String nodeAddress : nearest) {
                    String[] addrParts = nodeAddress.split(":");
                    sendCASRequest(key, currentValue, newValue,
                            InetAddress.getByName(addrParts[0]),
                            Integer.parseInt(addrParts[1]));
                }
                return true;
            }
            return false;
        }

        // Helper methods
        private String computeHashID(String key) {
            byte[] hashBytes = sha256.digest(key.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte hashByte : hashBytes) {
                String hex = Integer.toHexString(0xff & hashByte);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        }

        private byte[] generateTransactionID() {
            byte[] txID = new byte[2];
            do {
                random.nextBytes(txID);
            } while (txID[0] == 0x20 || txID[1] == 0x20);
            return txID;
        }

        private void sendUDPMessage(byte[] message, InetAddress address, int port) {
            try {
                DatagramPacket packet = new DatagramPacket(message, message.length, address, port);
                socket.send(packet);
            } catch (IOException e) {
                System.err.println("Error sending UDP message: " + e.getMessage());
            }
        }

        private void sendWithRetries(byte[] message, InetAddress address, int port) {
            String txKey = new String(Arrays.copyOfRange(message, 0, 2));
            pendingRequests.put(txKey, new PendingRequest(
                    message, address, port, System.currentTimeMillis() + 5000, 0));
            sendUDPMessage(message, address, port);
        }

        private List<String> findClosestNodes(String hashID, int count) {
            List<Map.Entry<String, Long>> nodes = new ArrayList<>(lastSeen.entrySet());
            nodes.sort((a, b) -> {
                int distA = calculateDistance(hashID, computeHashID(a.getKey()));
                int distB = calculateDistance(hashID, computeHashID(b.getKey()));
                return Integer.compare(distA, distB);
            });

            List<String> result = new ArrayList<>();
            for (int i = 0; i < Math.min(count, nodes.size()); i++) {
                result.add(knownNodes.get(nodes.get(i).getKey()));
            }
            return result;
        }

        private int calculateDistance(String hashID1, String hashID2) {
            BigInteger h1 = new BigInteger(hashID1, 16);
            BigInteger h2 = new BigInteger(hashID2, 16);
            return h1.xor(h2).bitLength();
        }

        // Message handlers
        private void handleNameRequest(byte[] txID, InetAddress address, int port) {
            String response = new String(txID) + " H " + nodeName + " ";
            sendUDPMessage(response.getBytes(), address, port);
        }

        private void handleNearestRequest(byte[] txID, String hashID, InetAddress address, int port) {
            List<String> closest = findClosestNodes(hashID, 3);
            StringBuilder response = new StringBuilder(new String(txID) + " O ");
            for (String nodeAddress : closest) {
                response.append(nodeAddress).append(" ");
            }
            sendUDPMessage(response.toString().getBytes(), address, port);
        }

        private void handleKeyExistenceRequest(byte[] txID, String key, InetAddress address, int port) {
            String hashID = computeHashID(key);
            char responseChar = dataStore.containsKey(hashID) ? 'Y' : 'N';
            String response = new String(txID) + " F " + responseChar + " ";
            sendUDPMessage(response.getBytes(), address, port);
        }

        private void handleReadRequest(byte[] txID, String key, InetAddress address, int port) {
            String hashID = computeHashID(key);
            String value = dataStore.get(hashID);
            char responseChar = value != null ? 'Y' : 'N';
            String response = new String(txID) + " S " + responseChar + " " + (value != null ? value : "") + " ";
            sendUDPMessage(response.getBytes(), address, port);
        }

        private void handleWriteRequest(byte[] txID, String payload, InetAddress address, int port) {
            String[] parts = payload.split(" ", 2);
            if (parts.length < 2) return;
            String hashID = computeHashID(parts[0]);
            dataStore.put(hashID, parts[1]);
            String response = new String(txID) + " X A ";
            sendUDPMessage(response.getBytes(), address, port);
        }

        private void handleCompareAndSwapRequest(byte[] txID, String payload, InetAddress address, int port) {
            String[] parts = payload.split(" ", 3);
            if (parts.length < 3) return;
            String hashID = computeHashID(parts[0]);
            char responseChar = dataStore.replace(hashID, parts[1], parts[2]) ? 'R' : 'N';
            String response = new String(txID) + " D " + responseChar + " ";
            sendUDPMessage(response.getBytes(), address, port);
        }

        private void handleRelayMessage(byte[] txID, String payload, InetAddress sender, int senderPort) {
            // Validate input parameters
            if (txID == null || payload == null || sender == null) {
                System.err.println("Invalid parameters in handleRelayMessage");
                return;
            }

            // Split payload into target node and inner message
            String[] parts = payload.split(" ", 2);
            if (parts.length < 2) {
                System.err.println("Malformed relay message payload");
                return;
            }

            String targetNode = parts[0];
            String innerMessage = parts[1];

            // Get target address from known nodes
            String targetAddress = knownNodes.get(targetNode);
            if (targetAddress == null) {
                System.err.println("Unknown target node: " + targetNode);
                return;
            }

            try {
                // Parse address and port
                String[] addrParts = targetAddress.split(":");
                if (addrParts.length != 2) {
                    System.err.println("Malformed address format: " + targetAddress);
                    return;
                }

                // Validate host address
                String host = addrParts[0];
                if (host == null || host.isEmpty()) {
                    System.err.println("Empty host address");
                    return;
                }

                // Validate port number
                int port;
                try {
                    port = Integer.parseInt(addrParts[1]);
                    if (port < 0 || port > 65535) {
                        System.err.println("Invalid port number: " + port);
                        return;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port format: " + addrParts[1]);
                    return;
                }

                // Create relay message
                byte[] relayedMessage = (new String(txID) + " " + innerMessage).getBytes(StandardCharsets.UTF_8);

                // Resolve host address with proper exception handling
                InetAddress targetIp;
                try {
                    targetIp = InetAddress.getByName(host);
                } catch (UnknownHostException e) {
                    System.err.println("Unknown host: " + host);
                    return;
                } catch (SecurityException e) {
                    System.err.println("Security violation resolving host: " + host);
                    return;
                }

                // Send the relayed message
                sendUDPMessage(relayedMessage, targetIp, port);

            } catch (Exception e) {
                System.err.println("Unexpected error handling relay message: " + e.getMessage());
            }
        }

        private void handleNameResponse(byte[] txID, String payload, InetAddress address, int port) {
            String nodeName = payload.trim();
            String addressStr = address.getHostAddress() + ":" + port;
            knownNodes.put(nodeName, addressStr);
            lastSeen.put(nodeName, System.currentTimeMillis());
        }

        private void handleNearestResponse(byte[] txID, String payload, InetAddress address, int port) {
            String[] entries = payload.split(" ");
            for (int i = 0; i < entries.length; i += 2) {
                if (i+1 < entries.length) {
                    String nodeName = entries[i];
                    String nodeAddress = entries[i+1];
                    knownNodes.put(nodeName, nodeAddress);
                    lastSeen.put(nodeName, System.currentTimeMillis());
                }
            }
        }

        private void handleReadResponse(byte[] txID, String payload, InetAddress address, int port) {
            // Handle read responses if needed
        }

        private String sendReadRequest(String key, InetAddress address, int port) throws Exception {
            byte[] txID = generateTransactionID();
            String requestMessage = new String(txID) + " R " + key + " ";
            sendWithRetries(requestMessage.getBytes(), address, port);
            // Simplified - would need proper response handling
            return null;
        }

        private void sendWriteRequest(String key, String value, InetAddress address, int port) throws Exception {
            byte[] txID = generateTransactionID();
            String requestMessage = new String(txID) + " W " + key + " " + value + " ";
            sendWithRetries(requestMessage.getBytes(), address, port);
        }

        private void sendCASRequest(String key, String currentValue, String newValue,
                                    InetAddress address, int port) throws Exception {
            byte[] txID = generateTransactionID();
            String requestMessage = new String(txID) + " C " + key + " " + currentValue + " " + newValue + " ";
            sendWithRetries(requestMessage.getBytes(), address, port);
        }
    }