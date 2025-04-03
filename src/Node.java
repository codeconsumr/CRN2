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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

class AddressEntry {
    String nodeName;
    String address;
    byte[] hashID;
    int distance;

    public AddressEntry(String nodeName, String address, byte[] hashID, int distance) {
        this.nodeName = nodeName;
        this.address = address;
        this.hashID = hashID;
        this.distance = distance;
    }
}

class PendingRequest {
    byte[] transactionID;
    long timestamp;
    int attempts;
    InetAddress address;
    int port;
    String originalMessage;
    CompletableFuture<String> future;

    public PendingRequest(byte[] transactionID, InetAddress address, int port, String originalMessage) {
        this.transactionID = transactionID;
        this.timestamp = System.currentTimeMillis();
        this.attempts = 1;
        this.address = address;
        this.port = port;
        this.originalMessage = originalMessage;
        this.future = new CompletableFuture<>();
    }
}

public class Node implements NodeInterface {
    // Node configuration
    private String nodeName;
    private byte[] nodeHashID;
    private DatagramSocket socket;
    private String myAddress;
    private int myPort;

    // Storage for key/value pairs
    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private final Map<String, List<AddressEntry>> addressesByDistance = new ConcurrentHashMap<>();
    private final Map<String, byte[]> hashIDCache = new ConcurrentHashMap<>();

    // Network communication
    private final Random random = new Random();
    private MessageDigest sha256;

    // Relay stack
    private final Deque<String> relayStack = new ConcurrentLinkedDeque<>();

    // Pending requests tracking
    private final Map<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final ScheduledExecutorService requestTimeoutService = Executors.newSingleThreadScheduledExecutor();

    // Processing
    private final ExecutorService messageProcessor = Executors.newFixedThreadPool(4);
    private volatile boolean running = false;

    public Node() {
        try {
            this.sha256 = MessageDigest.getInstance("SHA-256");

            // Start request timeout checker
            requestTimeoutService.scheduleAtFixedRate(this::checkPendingRequests, 1, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize node", e);
        }
    }

    private void checkPendingRequests() {
        long currentTime = System.currentTimeMillis();

        pendingRequests.values().forEach(request -> {
            if (currentTime - request.timestamp > 5000) {
                if (request.attempts < 4) {
                    // Resend the request
                    try {
                        request.timestamp = currentTime;
                        request.attempts++;
                        DatagramPacket packet = new DatagramPacket(
                                request.originalMessage.getBytes(StandardCharsets.UTF_8),
                                request.originalMessage.length(),
                                request.address,
                                request.port
                        );
                        socket.send(packet);
                    } catch (IOException e) {
                        request.future.completeExceptionally(e);
                        pendingRequests.remove(new String(request.transactionID, StandardCharsets.UTF_8));
                    }
                } else {
                    // Max retries reached
                    request.future.completeExceptionally(new TimeoutException("No response after 3 retries"));
                    pendingRequests.remove(new String(request.transactionID, StandardCharsets.UTF_8));
                }
            }
        });
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            nodeName = "N:" + nodeName;
        }
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        // Close existing socket if open
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }

        // Open new socket on specified port
        socket = new DatagramSocket(portNumber);
        myPort = portNumber;

        // Start message receiver thread
        running = true;
        new Thread(this::receiveMessages).start();
    }

    private void receiveMessages() {
        byte[] buffer = new byte[4096];

        while (running) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);

                // Copy the data so it doesn't get overwritten
                byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
                InetAddress senderAddress = packet.getAddress();
                int senderPort = packet.getPort();

                // Process the message in a separate thread
                messageProcessor.submit(() -> processMessage(data, senderAddress, senderPort));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error receiving message: " + e.getMessage());
                }
            }
        }
    }

    private void processMessage(byte[] messageData, InetAddress senderAddress, int senderPort) {
        String message = new String(messageData, StandardCharsets.UTF_8);

        try {
            // Extract transaction ID (first 2 bytes followed by a space)
            if (message.length() < 3 || message.charAt(2) != ' ') {
                // Invalid message format
                return;
            }

            String txIDStr = message.substring(0, 2);
            byte[] txID = txIDStr.getBytes(StandardCharsets.UTF_8);

            // Extract message type (single character after space)
            char messageType = message.charAt(3);

            // Passive mapping: if we get a request from a node, store its address
            if (messageType == 'G' || messageType == 'H') {
                // We might learn the node's name from a response
                if (messageType == 'H' && message.length() > 5) {
                    String[] parts = message.substring(5).split(" ", 3);
                    if (parts.length >= 1) {
                        String remoteName = parts[0];
                        if (remoteName.startsWith("N:")) {
                            String remoteAddress = senderAddress.getHostAddress() + ":" + senderPort;
                            storeAddressKeyValue(remoteName, remoteAddress);
                        }
                    }
                }
            }

            // Process by message type
            switch (messageType) {
                case 'G': // Name request
                    handleNameRequest(txID, senderAddress, senderPort);
                    break;

                case 'H': // Name response
                    handleNameResponse(txIDStr, message.substring(5), senderAddress, senderPort);
                    break;

                case 'N': // Nearest request
                    handleNearestRequest(txID, message.substring(5), senderAddress, senderPort);
                    break;

                case 'O': // Nearest response
                    handleNearestResponse(txIDStr, message.substring(5));
                    break;

                case 'E': // Key existence request
                    handleKeyExistenceRequest(txID, message.substring(5), senderAddress, senderPort);
                    break;

                case 'F': // Key existence response
                    handleKeyExistenceResponse(txIDStr, message.substring(5));
                    break;

                case 'R': // Read request
                    handleReadRequest(txID, message.substring(5), senderAddress, senderPort);
                    break;

                case 'S': // Read response
                    handleReadResponse(txIDStr, message.substring(5));
                    break;

                case 'W': // Write request
                    handleWriteRequest(txID, message.substring(5), senderAddress, senderPort);
                    break;

                case 'X': // Write response
                    handleWriteResponse(txIDStr, message.substring(5));
                    break;

                case 'C': // Compare and swap request
                    handleCompareAndSwapRequest(txID, message.substring(5), senderAddress, senderPort);
                    break;

                case 'D': // Compare and swap response
                    handleCompareAndSwapResponse(txIDStr, message.substring(5));
                    break;

                case 'V': // Relay message
                    handleRelayMessage(txID, message.substring(5), senderAddress, senderPort);
                    break;

                case 'I': // Information message
                    // Just log it
                    System.out.println("Information: " + message.substring(5));
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        // We're already handling messages in the background, just wait the requested time
        if (delay > 0) {
            Thread.sleep(delay);
        } else {
            // Wait indefinitely (until interrupted)
            synchronized (this) {
                this.wait();
            }
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            nodeName = "N:" + nodeName;
        }

        // Try to get the address
        AddressEntry entry = findAddressByNodeName(nodeName);
        if (entry == null) {
            return false;
        }

        // Send a name request and see if we get a valid response
        String[] addressParts = entry.address.split(":");
        if (addressParts.length != 2) {
            return false;
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        try {
            byte[] txID = generateTransactionID();
            String request = new String(txID, StandardCharsets.UTF_8) + " G";

            CompletableFuture<String> future = sendRequest(request, address, port);
            String response = future.get(10, TimeUnit.SECONDS);

            return response != null && response.charAt(0) == 'H' && response.contains(nodeName);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            nodeName = "N:" + nodeName;
        }
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
        byte[] keyHashID = getHashID(key);

        // If stored locally, check directly
        if (dataStore.containsKey(key)) {
            return true;
        }

        // Otherwise, find the nearest nodes and ask them
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);
        if (nearestNodes.isEmpty()) {
            return false;
        }

        // Check if we are one of the nearest nodes
        boolean weAreNearestNode = isNodeOneOfNearest(nodeHashID, keyHashID, nearestNodes);

        // If we're one of the nearest and we don't have it, it doesn't exist
        if (weAreNearestNode) {
            return false;
        }

        // Ask the nearest nodes
        for (AddressEntry node : nearestNodes) {
            try {
                String result = sendKeyExistenceRequest(key, node);
                if (result != null && result.startsWith("Y")) {
                    return true;
                }
            } catch (Exception e) {
                // Continue with next node
            }
        }

        return false;
    }

    @Override
    public String read(String key) throws Exception {
        byte[] keyHashID = getHashID(key);

        // 1. Check if stored locally first
        String localValue = dataStore.get(key);
        if (localValue != null) {
            return localValue;
        }

        // 2. Find the nearest nodes to this key's hash
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);

        // 3. If we don't know any nodes, try to find some by querying known nodes
        if (nearestNodes.isEmpty() || nearestNodes.size() < 3) {
            // Get some known nodes to ask about nearest nodes
            List<AddressEntry> knownNodes = getAllKnownNodes();
            if (!knownNodes.isEmpty()) {
                for (AddressEntry knownNode : knownNodes) {
                    try {
                        // Ask this node for its nearest nodes to our target
                        String hashIDHex = hashIDToHex(keyHashID);
                        List<AddressEntry> moreNodes = sendNearestRequest(hashIDHex, knownNode);

                        // Add any new nodes we discovered
                        for (AddressEntry newNode : moreNodes) {
                            if (!containsNode(nearestNodes, newNode.nodeName)) {
                                nearestNodes.add(newNode);
                            }
                        }

                        // Stop if we have found enough nodes
                        if (nearestNodes.size() >= 3) {
                            break;
                        }
                    } catch (Exception e) {
                        // Continue with next node if this one fails
                    }
                }
            }
        }

        // Sort by distance to the key
        Collections.sort(nearestNodes, Comparator.comparingInt(e -> calculateDistance(e.hashID, keyHashID)));

        // 4. Query each of the nearest nodes for the data
        for (AddressEntry node : nearestNodes) {
            try {
                String value = sendReadRequest(key, node);
                if (value != null) {
                    return value;
                }
            } catch (Exception e) {
                // Continue with next node
            }
        }

        return null;
    }

    // Helper method to get all known nodes from the address entries
    private List<AddressEntry> getAllKnownNodes() {
        List<AddressEntry> allNodes = new ArrayList<>();
        for (List<AddressEntry> entries : addressesByDistance.values()) {
            allNodes.addAll(entries);
        }
        return allNodes;
    }

    // Helper method to check if a list contains a node with the given name
    private boolean containsNode(List<AddressEntry> nodes, String nodeName) {
        for (AddressEntry node : nodes) {
            if (node.nodeName.equals(nodeName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        byte[] keyHashID = getHashID(key);

        // For address keys, handle specially
        if (key.startsWith("N:")) {
            // Store locally
            storeAddressKeyValue(key, value);

            // Also distribute to some other nodes to increase connectivity
            List<AddressEntry> randomNodes = getRandomAddressEntries(3);
            for (AddressEntry node : randomNodes) {
                try {
                    sendWriteRequest(key, value, node);
                } catch (Exception e) {
                    // Ignore failures
                }
            }
            return true;
        }

        // For data keys, find the 3 nearest nodes
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);

        // If we don't know any nodes, store locally
        if (nearestNodes.isEmpty()) {
            dataStore.put(key, value);
            return true;
        }

        // Check if we are one of the nearest nodes
        boolean weAreNearestNode = isNodeOneOfNearest(nodeHashID, keyHashID, nearestNodes);

        // If we're one of the nearest, store locally
        if (weAreNearestNode) {
            dataStore.put(key, value);
        }

        // Try to store on nearest nodes
        boolean success = false;
        for (AddressEntry node : nearestNodes) {
            try {
                boolean nodeSuccess = sendWriteRequest(key, value, node);
                success = success || nodeSuccess;
            } catch (Exception e) {
                // Continue with next node
            }
        }

        return success || weAreNearestNode;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        byte[] keyHashID = getHashID(key);

        // If stored locally, handle directly
        if (dataStore.containsKey(key)) {
            synchronized (dataStore) {
                if (dataStore.get(key).equals(currentValue)) {
                    dataStore.put(key, newValue);
                    return true;
                } else {
                    return false;
                }
            }
        }

        // Otherwise, find the nearest nodes and ask them
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);
        if (nearestNodes.isEmpty()) {
            return false;
        }

        // Try CAS on nearest nodes
        for (AddressEntry node : nearestNodes) {
            try {
                boolean success = sendCASRequest(key, currentValue, newValue, node);
                if (success) {
                    return true;
                }
            } catch (Exception e) {
                // Continue with next node
            }
        }

        return false;
    }

    // Helper methods for CRN protocol handling

    private byte[] generateTransactionID() {
        byte[] txID = new byte[2];
        do {
            random.nextBytes(txID);
        } while (txID[0] == 0x20 || txID[1] == 0x20); // Ensure no spaces
        return txID;
    }

    private byte[] getHashID(String key) {
        if (hashIDCache.containsKey(key)) {
            return hashIDCache.get(key);
        }

        try {
            byte[] hashID = HashID.computeHashID(key);
            hashIDCache.put(key, hashID);
            return hashID;
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute hashID for " + key, e);
        }
    }

    private String hashIDToHex(byte[] hashID) {
        StringBuilder hex = new StringBuilder();
        for (byte b : hashID) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    private int calculateDistance(byte[] hashID1, byte[] hashID2) {
        // Calculate the distance as per RFC (256 - number of leading matching bits)
        int leadingMatchingBits = 0;

        for (int i = 0; i < hashID1.length; i++) {
            byte xor = (byte) (hashID1[i] ^ hashID2[i]);

            if (xor == 0) {
                leadingMatchingBits += 8;
            } else {
                // Find the position of the first 1 bit
                for (int j = 7; j >= 0; j--) {
                    if ((xor & (1 << j)) == 0) {
                        leadingMatchingBits++;
                    } else {
                        break;
                    }
                }
                break;
            }
        }

        return 256 - leadingMatchingBits;
    }

    private String formatString(String str) {
        int spaceCount = 0;
        for (char c : str.toCharArray()) {
            if (c == ' ') spaceCount++;
        }
        return spaceCount + " " + str + " ";
    }

    private String parseString(String formattedStr) {
        String[] parts = formattedStr.split(" ", 3);
        if (parts.length < 3) {
            return "";
        }
        return parts[1];
    }

    private void storeAddressKeyValue(String nodeNameKey, String addressValue) {
        try {
            if (!nodeNameKey.startsWith("N:")) {
                return;
            }

            byte[] hashID = getHashID(nodeNameKey);
            int distance = calculateDistance(nodeHashID, hashID);

            // Create or get the list for this distance
            List<AddressEntry> entriesAtDistance = addressesByDistance.computeIfAbsent(
                    String.valueOf(distance),
                    k -> new CopyOnWriteArrayList<>()
            );

            // Check if we already have this node
            for (AddressEntry entry : entriesAtDistance) {
                if (entry.nodeName.equals(nodeNameKey)) {
                    // Update the address
                    entry.address = addressValue;
                    return;
                }
            }

            // Add new entry
            entriesAtDistance.add(new AddressEntry(nodeNameKey, addressValue, hashID, distance));

            // Limit to 3 entries per distance
            if (entriesAtDistance.size() > 3) {
                // Remove oldest (or implement your own policy)
                entriesAtDistance.remove(0);
            }
        } catch (Exception e) {
            System.err.println("Error storing address key/value: " + e.getMessage());
        }
    }

    private AddressEntry findAddressByNodeName(String nodeName) {
        for (List<AddressEntry> entries : addressesByDistance.values()) {
            for (AddressEntry entry : entries) {
                if (entry.nodeName.equals(nodeName)) {
                    return entry;
                }
            }
        }
        return null;
    }

    private List<AddressEntry> findNearestNodes(byte[] targetHashID, int count) {
        // Create a sorted list of all address entries by distance to target
        List<AddressEntry> allEntries = new ArrayList<>();

        for (List<AddressEntry> entries : addressesByDistance.values()) {
            for (AddressEntry entry : entries) {
                int distance = calculateDistance(targetHashID, entry.hashID);
                entry.distance = distance;
                allEntries.add(entry);
            }
        }

        // Sort by distance
        Collections.sort(allEntries, Comparator.comparingInt(e -> e.distance));

        // Return the closest 'count' entries
        return allEntries.stream()
                .limit(count)
                .collect(Collectors.toList());
    }

    private boolean isNodeOneOfNearest(byte[] nodeHashID, byte[] targetHashID, List<AddressEntry> nearestNodes) {
        int ourDistance = calculateDistance(nodeHashID, targetHashID);

        for (AddressEntry entry : nearestNodes) {
            if (entry.distance > ourDistance) {
                return true;
            }
        }

        return nearestNodes.size() < 3;
    }

    private List<AddressEntry> getRandomAddressEntries(int count) {
        List<AddressEntry> allEntries = new ArrayList<>();

        for (List<AddressEntry> entries : addressesByDistance.values()) {
            allEntries.addAll(entries);
        }

        if (allEntries.isEmpty()) {
            return Collections.emptyList();
        }

        Collections.shuffle(allEntries);
        return allEntries.stream()
                .limit(Math.min(count, allEntries.size()))
                .collect(Collectors.toList());
    }

    private CompletableFuture<String> sendRequest(String message, InetAddress address, int port) {
        try {
            byte[] txID = message.substring(0, 2).getBytes(StandardCharsets.UTF_8);
            String txIDStr = new String(txID, StandardCharsets.UTF_8);

            // Create pending request
            PendingRequest pendingRequest = new PendingRequest(txID, address, port, message);
            pendingRequests.put(txIDStr, pendingRequest);

            // Send the message
            DatagramPacket packet = new DatagramPacket(
                    message.getBytes(StandardCharsets.UTF_8),
                    message.length(),
                    address,
                    port
            );
            socket.send(packet);

            return pendingRequest.future;
        } catch (IOException e) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    private void sendResponse(byte[] txID, String responseData, InetAddress address, int port) {
        try {
            String response = new String(txID, StandardCharsets.UTF_8) + responseData;
            DatagramPacket packet = new DatagramPacket(
                    response.getBytes(StandardCharsets.UTF_8),
                    response.length(),
                    address,
                    port
            );
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }

    // Message handlers

    private void handleNameRequest(byte[] txID, InetAddress address, int port) {
        String response = " H " + formatString(nodeName);
        sendResponse(txID, response, address, port);
    }

    private void handleNameResponse(String txIDStr, String responseData, InetAddress address, int port) {
        PendingRequest request = pendingRequests.remove(txIDStr);
        if (request != null) {
            request.future.complete(responseData);
        }
    }

    private void handleNearestRequest(byte[] txID, String hashIDHex, InetAddress address, int port) {
        // Convert hex to byte array
        byte[] targetHashID = new byte[32];
        for (int i = 0; i < 32; i++) {
            targetHashID[i] = (byte) Integer.parseInt(hashIDHex.substring(i * 2, i * 2 + 2), 16);
        }

        // Find nearest nodes
        List<AddressEntry> nearestNodes = findNearestNodes(targetHashID, 3);

        // Format response
        StringBuilder response = new StringBuilder(" O ");

        for (AddressEntry entry : nearestNodes) {
            response.append("0 ")
                    .append(entry.nodeName)
                    .append(" 0 ")
                    .append(entry.address)
                    .append(" ");
        }

        sendResponse(txID, response.toString(), address, port);
    }

    private void handleNearestResponse(String txIDStr, String responseData) {
        PendingRequest request = pendingRequests.remove(txIDStr);
        if (request != null) {
            request.future.complete(responseData);

            // Parse and store address entries
            String[] entries = responseData.split(" 0 ");
            for (int i = 1; i < entries.length; i += 2) {
                try {
                    String nodeName = entries[i].trim();
                    String address = entries[i + 1].trim();

                    if (nodeName.startsWith("N:")) {
                        storeAddressKeyValue(nodeName, address);
                    }
                } catch (IndexOutOfBoundsException e) {
                    // Skip invalid entries
                }
            }
        }
    }

    private void handleKeyExistenceRequest(byte[] txID, String key, InetAddress address, int port) {
        byte[] keyHashID = getHashID(key);

        // Check if we have the key
        if (dataStore.containsKey(key)) {
            sendResponse(txID, " F Y", address, port);
            return;
        }

        // Check if we're one of the nearest nodes
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);
        boolean weAreNearestNode = isNodeOneOfNearest(nodeHashID, keyHashID, nearestNodes);

        if (weAreNearestNode) {
            sendResponse(txID, " F N", address, port);
        } else {
            sendResponse(txID, " F ?", address, port);
        }
    }

    private void handleKeyExistenceResponse(String txIDStr, String responseData) {
        PendingRequest request = pendingRequests.remove(txIDStr);
        if (request != null) {
            request.future.complete(responseData);
        }
    }

    private void handleReadRequest(byte[] txID, String key, InetAddress address, int port) {
        byte[] keyHashID = getHashID(key);

        // Check if we have the key
        String value = dataStore.get(key);
        if (value != null) {
            String response = " S Y " + formatString(value);
            sendResponse(txID, response, address, port);
            return;
        }

        // Check if we're one of the nearest nodes
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);
        boolean weAreNearestNode = isNodeOneOfNearest(nodeHashID, keyHashID, nearestNodes);

        if (weAreNearestNode) {
            sendResponse(txID, " S N 0  ", address, port);
        } else {
            sendResponse(txID, " S ? 0  ", address, port);
        }
    }

    private void handleReadResponse(String txIDStr, String responseData) {
        PendingRequest request = pendingRequests.remove(txIDStr);
        if (request != null) {
            request.future.complete(responseData);
        }
    }

    private void handleWriteRequest(byte[] txID, String keyValueData, InetAddress address, int port) {
        // Parse the key and value
        String[] parts = keyValueData.split(" ", 4);
        if (parts.length < 4) {
            // Invalid format
            sendResponse(txID, " X X", address, port);
            return;
        }

        int keySpaces = Integer.parseInt(parts[0]);
        String key = parts[1];

        // Value starts after key and its space
        String valueStr = keyValueData.substring(parts[0].length() + 1 + key.length() + 1);
        String value = parseString(valueStr);

        byte[] keyHashID = getHashID(key);

        // Check if we already have this key
        boolean hasKey = dataStore.containsKey(key);

        // Check if we're one of the nearest nodes
        List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);
        boolean weAreNearestNode = isNodeOneOfNearest(nodeHashID, keyHashID, nearestNodes);

        if (hasKey) {
            // Replace existing value
            dataStore.put(key, value);
            sendResponse(txID, " X R", address, port);
        } else if (weAreNearestNode) {
            // We're one of the three closest nodes, so accept the write
            dataStore.put(key, value);
            sendResponse(txID, " X A", address, port);
        } else {
            // We're not one of the three closest nodes, so reject
            sendResponse(txID, " X X", address, port);
        }
    }

    private void handleWriteResponse(String txIDStr, String responseData) {
        PendingRequest request = pendingRequests.remove(txIDStr);
        if (request != null) {
            request.future.complete(responseData);
        }
    }

    private void handleCompareAndSwapRequest(byte[] txID, String casData, InetAddress address, int port) {
        try {
            // Parse key and the two values
            String[] initialParts = casData.split(" ", 4);
            if (initialParts.length < 4) {
                sendResponse(txID, " D X", address, port);
                return;
            }

            int keySpaces = Integer.parseInt(initialParts[0]);
            String key = initialParts[1];

            // Extract the part containing the current value and new value
            String valuesStr = casData.substring(initialParts[0].length() + 1 + key.length() + 1);

            // Parse current value
            String[] valueParts = valuesStr.split(" ", 2);
            int currentValueSpaces = Integer.parseInt(valueParts[0]);

            String currentValueWithSpace = valueParts[1];
            String[] currentValueParts = currentValueWithSpace.split(" ", currentValueSpaces + 2);
            StringBuilder currentValueBuilder = new StringBuilder();
            for (int i = 0; i < currentValueSpaces + 1; i++) {
                currentValueBuilder.append(currentValueParts[i]);
                if (i < currentValueSpaces) {
                    currentValueBuilder.append(" ");
                }
            }
            String currentValue = currentValueBuilder.toString();

            // Extract the new value part
            String newValuePart = valuesStr.substring(valueParts[0].length() + 1 + currentValue.length() + 1);
            String newValue = parseString(newValuePart);

            byte[] keyHashID = getHashID(key);

            // Check if we have the key
            boolean hasKey = dataStore.containsKey(key);

            // Check if we're one of the nearest nodes
            List<AddressEntry> nearestNodes = findNearestNodes(keyHashID, 3);
            boolean weAreNearestNode = isNodeOneOfNearest(nodeHashID, keyHashID, nearestNodes);

            if (hasKey) {
                // Synchronize to ensure atomicity of CAS operation
                synchronized (dataStore) {
                    String storedValue = dataStore.get(key);
                    if (storedValue.equals(currentValue)) {
                        dataStore.put(key, newValue);
                        sendResponse(txID, " D R", address, port);
                    } else {
                        sendResponse(txID, " D N", address, port);
                    }
                }
            } else if (weAreNearestNode) {
                // We're one of the three closest nodes, so accept the write with new value
                dataStore.put(key, newValue);
                sendResponse(txID, " D A", address, port);
            } else {
                // We're not one of the three closest nodes, so reject
                sendResponse(txID, " D X", address, port);
            }
        } catch (Exception e) {
            System.err.println("Error handling CAS request: " + e.getMessage());
            sendResponse(txID, " D X", address, port);
        }
    }

    private void handleCompareAndSwapResponse(String txIDStr, String responseData) {
        PendingRequest request = pendingRequests.remove(txIDStr);
        if (request != null) {
            request.future.complete(responseData);
        }
    }

    private void handleRelayMessage(byte[] txID, String relayMessage, InetAddress address, int port) {
        try {
            // Format: nodeName message
            // Extract the node name and the message to relay
            String[] parts = relayMessage.split(" ", 4);
            if (parts.length < 4) {
                return;
            }

            int nodeNameSpaces = Integer.parseInt(parts[0]);
            String nodeName = parts[1];

            // Calculate the start index for the relayed message
            // Convert the index calculation to an integer
            int relayedMessageStart = parts[0].length() + 1 + nodeName.length() + 1;
            String messageToRelay = relayMessage.substring(relayedMessageStart);

            // Find the address for the target node
            AddressEntry targetNode = findAddressByNodeName(nodeName);
            if (targetNode == null) {
                // Can't relay if we don't know the node
                return;
            }

            // Parse the target address
            String[] addressParts = targetNode.address.split(":");
            if (addressParts.length != 2) {
                return;
            }

            InetAddress targetAddress = InetAddress.getByName(addressParts[0]);
            int targetPort = Integer.parseInt(addressParts[1]);

            // Extract the transaction ID from the message to relay
            byte[] relayTxID = messageToRelay.substring(0, 2).getBytes(StandardCharsets.UTF_8);
            char messageType = messageToRelay.charAt(3);

            // Send the message to the target node
            DatagramPacket packet = new DatagramPacket(
                    messageToRelay.getBytes(StandardCharsets.UTF_8),
                    messageToRelay.length(),
                    targetAddress,
                    targetPort
            );
            socket.send(packet);

            // If this is a request message, we need to wait for a response and relay it back
            if (messageType == 'G' || messageType == 'N' || messageType == 'E' ||
                    messageType == 'R' || messageType == 'W' || messageType == 'C') {

                // Store the original transaction ID and sender info to relay the response back
                String relayTxIDStr = new String(relayTxID, StandardCharsets.UTF_8);
                PendingRequest pendingRelay = new PendingRequest(txID, address, port, "");
                pendingRequests.put(relayTxIDStr, pendingRelay);
            }
        } catch (Exception e) {
            System.err.println("Error handling relay message: " + e.getMessage());
        }
    }

// Helper methods for sending protocol-specific requests

    private String sendNameRequest(AddressEntry node) throws Exception {
        String[] addressParts = node.address.split(":");
        if (addressParts.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + node.address);
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        String request = constructRequest("G");
        CompletableFuture<String> future = sendRequestWithRelay(request, address, port);

        String response = future.get(15, TimeUnit.SECONDS);
        if (response != null && response.startsWith("H")) {
            String[] parts = response.split(" ", 3);
            if (parts.length >= 3) {
                return parseString(parts[1] + " " + parts[2]);
            }
        }

        return null;
    }

    private List<AddressEntry> sendNearestRequest(String hashIDHex, AddressEntry node) throws Exception {
        String[] addressParts = node.address.split(":");
        if (addressParts.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + node.address);
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        String request = constructRequest("N " + hashIDHex);
        CompletableFuture<String> future = sendRequestWithRelay(request, address, port);

        String response = future.get(15, TimeUnit.SECONDS);
        List<AddressEntry> result = new ArrayList<>();

        if (response != null && response.startsWith("O")) {
            // Parse the node addresses - this is key/value format as described in the RFC
            try {
                String addressesSection = response.substring(2).trim();

                // Track parsing position
                int pos = 0;
                while (pos < addressesSection.length()) {
                    // Parse node name
                    int spaceCountEndPos = addressesSection.indexOf(' ', pos);
                    if (spaceCountEndPos == -1) break;

                    int nodeNameSpaces = Integer.parseInt(addressesSection.substring(pos, spaceCountEndPos));
                    int nodeNameStart = spaceCountEndPos + 1;

                    // Find node name end (after the specified number of spaces)
                    int nodeNameEnd = nodeNameStart;
                    int spacesFound = 0;
                    while (nodeNameEnd < addressesSection.length() && spacesFound <= nodeNameSpaces) {
                        if (addressesSection.charAt(nodeNameEnd) == ' ') {
                            spacesFound++;
                        }
                        nodeNameEnd++;
                    }

                    if (nodeNameEnd > addressesSection.length()) break;

                    String nodeName = addressesSection.substring(nodeNameStart, nodeNameEnd - 1);

                    // Parse address using same technique
                    pos = nodeNameEnd;
                    spaceCountEndPos = addressesSection.indexOf(' ', pos);
                    if (spaceCountEndPos == -1) break;

                    int addressSpaces = Integer.parseInt(addressesSection.substring(pos, spaceCountEndPos));
                    int addressStart = spaceCountEndPos + 1;

                    int addressEnd = addressStart;
                    spacesFound = 0;
                    while (addressEnd < addressesSection.length() && spacesFound <= addressSpaces) {
                        if (addressesSection.charAt(addressEnd) == ' ') {
                            spacesFound++;
                        }
                        addressEnd++;
                    }

                    if (addressEnd > addressesSection.length()) break;

                    String nodeAddress = addressesSection.substring(addressStart, addressEnd - 1);

                    if (nodeName.startsWith("N:")) {
                        byte[] hashID = getHashID(nodeName);
                        result.add(new AddressEntry(nodeName, nodeAddress, hashID, 0));

                        // Also store this address for future use
                        storeAddressKeyValue(nodeName, nodeAddress);
                    }

                    // Move to next pair
                    pos = addressEnd;
                }
            } catch (Exception e) {
                System.err.println("Error parsing nearest response: " + e.getMessage());
            }
        }

        return result;
    }

    private String sendKeyExistenceRequest(String key, AddressEntry node) throws Exception {
        String[] addressParts = node.address.split(":");
        if (addressParts.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + node.address);
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        String request = constructRequest("E " + key);
        CompletableFuture<String> future = sendRequestWithRelay(request, address, port);

        String response = future.get(15, TimeUnit.SECONDS);
        if (response != null && response.startsWith("F")) {
            return response.substring(2).trim();
        }

        return null;
    }

    private String sendReadRequest(String key, AddressEntry node) throws Exception {
        String[] addressParts = node.address.split(":");
        if (addressParts.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + node.address);
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        String request = constructRequest("R " + key);
        CompletableFuture<String> future = sendRequestWithRelay(request, address, port);

        String response = future.get(15, TimeUnit.SECONDS);
        if (response != null && response.startsWith("S")) {
            String[] parts = response.substring(2).split(" ", 3);
            if (parts.length >= 3 && parts[0].equals("Y")) {
                return parseString(parts[1] + " " + parts[2]);
            }
        }

        return null;
    }

    private boolean sendWriteRequest(String key, String value, AddressEntry node) throws Exception {
        String[] addressParts = node.address.split(":");
        if (addressParts.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + node.address);
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        String request = constructRequest("W " + formatString(key) + formatString(value));
        CompletableFuture<String> future = sendRequestWithRelay(request, address, port);

        String response = future.get(15, TimeUnit.SECONDS);
        if (response != null && response.startsWith("X")) {
            String result = response.substring(2).trim();
            return result.equals("R") || result.equals("A");
        }

        return false;
    }

    private boolean sendCASRequest(String key, String currentValue, String newValue, AddressEntry node) throws Exception {
        String[] addressParts = node.address.split(":");
        if (addressParts.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + node.address);
        }

        InetAddress address = InetAddress.getByName(addressParts[0]);
        int port = Integer.parseInt(addressParts[1]);

        String request = constructRequest("C " + formatString(key) + formatString(currentValue) + formatString(newValue));
        CompletableFuture<String> future = sendRequestWithRelay(request, address, port);

        String response = future.get(15, TimeUnit.SECONDS);
        if (response != null && response.startsWith("D")) {
            String result = response.substring(2).trim();
            return result.equals("R") || result.equals("A");
        }

        return false;
    }

    private String constructRequest(String requestContent) {
        byte[] txID = generateTransactionID();
        return new String(txID, StandardCharsets.UTF_8) + " " + requestContent;
    }

    private CompletableFuture<String> sendRequestWithRelay(String request, InetAddress address, int port) {
        // If relay stack is empty, send directly
        if (relayStack.isEmpty()) {
            return sendRequest(request, address, port);
        }

        // We need to relay through the stack
        try {
            Iterator<String> it = relayStack.iterator();
            String message = request;

            // Build relay message chain from the bottom of the stack up
            while (it.hasNext()) {
                String relayNodeName = it.next();
                byte[] txID = generateTransactionID();
                message = new String(txID, StandardCharsets.UTF_8) + " V " + formatString(relayNodeName) + message;
            }

            // Find the first relay node
            String firstRelayNodeName = relayStack.getLast();
            AddressEntry firstRelay = findAddressByNodeName(firstRelayNodeName);

            if (firstRelay == null) {
                CompletableFuture<String> future = new CompletableFuture<>();
                future.completeExceptionally(new Exception("First relay node not found: " + firstRelayNodeName));
                return future;
            }

            // Send to the first relay node
            String[] relayAddressParts = firstRelay.address.split(":");
            InetAddress relayAddress = InetAddress.getByName(relayAddressParts[0]);
            int relayPort = Integer.parseInt(relayAddressParts[1]);

            return sendRequest(message, relayAddress, relayPort);
        } catch (Exception e) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    // Cleanup method to call before shutting down
    public void shutdown() {
        running = false;
        requestTimeoutService.shutdown();
        messageProcessor.shutdown();

        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    // HashID utility class
    private static class HashID {
        public static byte[] computeHashID(String key) throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(key.getBytes(StandardCharsets.UTF_8));
        }
    }
}