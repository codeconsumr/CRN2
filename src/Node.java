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
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

public class Node implements NodeInterface {
    // Node properties
    private String nodeName;
    private DatagramSocket socket;
    private Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    private Map<String, InetSocketAddress> nodeAddresses = new ConcurrentHashMap<>();
    private Stack<String> relayStack = new Stack<>();
    private int sequenceNumber = new Random().nextInt(100);
    private Map<String, ResponseHandler> pendingRequests = new ConcurrentHashMap<>();

    // Constants
    private static final int MAX_PACKET_SIZE = 1024;
    private static final int REQUEST_TIMEOUT = 5000; // 5 seconds as per RFC
    private static final int MAX_RETRIES = 3;

    // Background thread for handling incoming packets
    private Thread listenerThread;
    private volatile boolean running = true;

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || nodeName.isEmpty() || !nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'");
        }
        this.nodeName = nodeName;
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        if (portNumber < 20110 || portNumber > 20130) {
            throw new IllegalArgumentException("Port number should be in range 20110-20130");
        }

        if (nodeName == null) {
            throw new IllegalStateException("Node name must be set before opening port");
        }

        // Create and configure the socket
        socket = new DatagramSocket(portNumber);

        // Store our own address in the key/value store
        try {
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            keyValueStore.put(nodeName, hostAddress + ":" + portNumber);
        } catch (UnknownHostException e) {
            // If we can't determine our address, use loopback
            keyValueStore.put(nodeName, "127.0.0.1:" + portNumber);
        }

        // Start the background listener thread
        startListenerThread();
    }

    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            byte[] buffer = new byte[MAX_PACKET_SIZE];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (running && !socket.isClosed()) {
                try {
                    // Reset the buffer for the next packet
                    Arrays.fill(buffer, (byte) 0);
                    packet.setLength(buffer.length);

                    // Wait for a packet
                    socket.receive(packet);

                    // Process the packet in a new thread to handle multiple concurrent requests
                    final DatagramPacket finalPacket = new DatagramPacket(
                            Arrays.copyOf(packet.getData(), packet.getLength()),
                            packet.getLength(),
                            packet.getAddress(),
                            packet.getPort());

                    new Thread(() -> {
                        try {
                            processPacket(finalPacket);
                        } catch (Exception e) {
                            // Silently handle errors in packet processing
                        }
                    }).start();

                } catch (IOException e) {
                    if (running && !socket.isClosed()) {
                        // Only log if this wasn't due to socket closing
                        System.err.println("Error receiving packet: " + e.getMessage());
                    }
                }
            }
        });

        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        System.out.println("[DEBUG-HANDLE] Handling incoming messages with delay: " + delay);
        if (delay <= 0) {
            System.out.println("[DEBUG-HANDLE] Delay <= 0, sleeping for 30 seconds");
            // Wait for 30 seconds when delay is 0 or negative
            Thread.sleep(30000);
        } else {
            System.out.println("[DEBUG-HANDLE] Sleeping for " + delay + " ms");
            // Wait for the specified delay
            Thread.sleep(delay);
        }
        System.out.println("[DEBUG-HANDLE] After sleep, known nodes: " + nodeAddresses.keySet());
    }

    private void processPacket(DatagramPacket packet) {
        try {
            String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            InetAddress sourceAddress = packet.getAddress();
            int sourcePort = packet.getPort();

            System.out.println("[DEBUG-PACKET] Received packet from: " + sourceAddress.getHostAddress() + ":" + sourcePort);
            System.out.println("[DEBUG-PACKET] Message: " + message);

            // Extract and store sender information for passive mapping
            storeNodeInfo(message, sourceAddress, sourcePort);

            // Process the message
            String[] parts = message.split(" ", 3);
            if (parts.length < 2) {
                System.out.println("[DEBUG-PACKET] Invalid message format, parts < 2");
                return; // Invalid message format
            }

            String seqId = parts[0];
            String command = parts[1];
            String payload = (parts.length > 2) ? parts[2] : "";

            processMessage(seqId, command, payload, sourceAddress, sourcePort);

        } catch (Exception e) {
            System.out.println("[DEBUG-PACKET] Error processing packet: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private void storeNodeInfo(String message, InetAddress address, int port) {
        try {
            // Look for node names in the message (starting with "N:")
            String[] parts = message.split(" ");
            for (String part : parts) {
                if (part.startsWith("N:")) {
                    System.out.println("[DEBUG-NODE] Found node in message: " + part + " at " + address.getHostAddress() + ":" + port);
                    nodeAddresses.put(part, new InetSocketAddress(address, port));
                    // Also store in key/value store if not already present
                    if (!keyValueStore.containsKey(part)) {
                        keyValueStore.put(part, address.getHostAddress() + ":" + port);
                        System.out.println("[DEBUG-NODE] Stored node in key-value store: " + part);
                    }
                    System.out.println("[DEBUG-NODE] Known nodes now: " + nodeAddresses.keySet());
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("[DEBUG-NODE] Error storing node info: " + e.getMessage());
        }
    }
    private void processMessage(String seqId, String command, String payload,
                                InetAddress sourceAddress, int sourcePort) {
        try {
            System.out.println("[DEBUG] Processing message: " + seqId + " " + command + " " +
                    (payload.length() > 20 ? payload.substring(0, 20) + "..." : payload));

            switch (command) {
                case "G": // Name request
                    handleNameRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "H": // Name response
                    handleNameResponse(seqId, payload);
                    break;
                case "N": // Nearest request
                    handleNearestRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "O": // Nearest response
                    handleNearestResponse(seqId, payload);
                    break;
                case "E": // Exists request
                    handleExistsRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "F": // Exists response
                    handleExistsResponse(seqId, payload);
                    break;
                case "R": // Read request
                    handleReadRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "S": // Read response
                    handleReadResponse(seqId, payload);
                    break;
                case "W": // Write request
                    handleWriteRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "X": // Write response
                    handleWriteResponse(seqId, payload);
                    break;
                case "C": // CAS request
                    handleCASRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "D": // CAS response
                    handleCASResponse(seqId, payload);
                    break;
                case "V": // Relay/forward request
                    handleRelayRequest(seqId, payload, sourceAddress, sourcePort);
                    break;
                case "I": // Information message
                    // Just log it
                    System.out.println("INFO: " + payload);
                    break;
                default:
                    System.out.println("[DEBUG] Unknown command: " + command);
                    break;
            }
        } catch (Exception e) {
            System.out.println("[DEBUG] Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Request Handlers

    private void handleNameRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            System.out.println("[DEBUG] Received name request from " + sourceAddress.getHostAddress() + ":" + sourcePort + " (seqId: " + seqId + ")");

            // Format response as per RFC section 6.1
            String response = seqId + " H " + formatString(nodeName);

            System.out.println("[DEBUG] Sending name response: " + response);
            sendPacket(response, sourceAddress, sourcePort);
        } catch (Exception e) {
            System.out.println("[DEBUG] Error handling name request: " + e.getMessage());
        }
    }

    private void handleNearestRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            System.out.println("[DEBUG] Received nearest request from " + sourceAddress.getHostAddress() + ":" + sourcePort + " for hash: " + payload);

            String hashIdHex = payload.trim();
            byte[] targetHashId = hexStringToByteArray(hashIdHex);

            // Find the closest nodes to the target hash
            List<Map.Entry<String, String>> closestNodes = findClosestNodes(targetHashId, 3);

            System.out.println("[DEBUG] Found " + closestNodes.size() + " closest nodes to hash");

            // Build the response containing up to 3 closest nodes
            StringBuilder response = new StringBuilder(seqId + " O");

            for (Map.Entry<String, String> entry : closestNodes) {
                String nodeName = entry.getKey();
                String nodeAddress = entry.getValue();

                System.out.println("[DEBUG] Including node in response: " + nodeName + " at " + nodeAddress);

                response.append(" ")
                        .append(formatString(nodeName))
                        .append(formatString(nodeAddress));
            }

            System.out.println("[DEBUG] Sending nearest response: " + response.toString());
            sendPacket(response.toString(), sourceAddress, sourcePort);
        } catch (Exception e) {
            System.out.println("[DEBUG] Error handling nearest request: " + e.getMessage());
        }
    }

    private void handleExistsRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            // Extract the key from the formatted string
            String key = extractString(payload);

            // Determine the response based on conditions A and B
            char responseChar;

            // Condition A: Does the node have a key/value pair whose key matches the requested key?
            if (keyValueStore.containsKey(key)) {
                responseChar = 'Y';
            }
            // Condition B: Is the node one of the three closest nodes to the requested key?
            else if (isAmongClosestNodes(key, 3)) {
                responseChar = 'N';
            }
            else {
                responseChar = '?';
            }

            // Send the response
            String response = seqId + " F " + responseChar;
            sendPacket(response, sourceAddress, sourcePort);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleReadRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            // Extract the key from the formatted string
            String key = extractString(payload);

            // Determine the response based on conditions A and B
            String response;

            // Condition A: Does the node have a key/value pair whose key matches the requested key?
            if (keyValueStore.containsKey(key)) {
                String value = keyValueStore.get(key);
                response = seqId + " S Y " + formatString(value);
            }
            // Condition B: Is the node one of the three closest nodes to the requested key?
            else if (isAmongClosestNodes(key, 3)) {
                response = seqId + " S N " + formatString("");
            }
            else {
                response = seqId + " S ? " + formatString("");
            }

            sendPacket(response, sourceAddress, sourcePort);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleWriteRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            // Parse the key and value from the formatted string
            // Format: "spaces key spaces value"
            String[] parts = splitStringFormat(payload);
            if (parts.length < 2) {
                return; // Invalid format
            }

            String key = parts[0];
            String value = parts[1];

            // Determine the response based on conditions A and B
            char responseChar;

            // Condition A: Does the node have a key/value pair whose key matches the requested key?
            if (keyValueStore.containsKey(key)) {
                keyValueStore.put(key, value);
                responseChar = 'R';

                // If this is a node address, update the nodeAddresses map too
                if (key.startsWith("N:")) {
                    updateNodeAddress(key, value);
                }
            }
            // Condition B: Is the node one of the three closest nodes to the requested key?
            else if (isAmongClosestNodes(key, 3)) {
                keyValueStore.put(key, value);
                responseChar = 'A';

                // If this is a node address, update the nodeAddresses map too
                if (key.startsWith("N:")) {
                    updateNodeAddress(key, value);
                }
            }
            else {
                responseChar = 'X';
            }

            // Send the response
            String response = seqId + " X " + responseChar;
            sendPacket(response, sourceAddress, sourcePort);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleCASRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            // Parse the key, expected value, and new value
            // Format: "spaces key spaces expectedValue spaces newValue"
            String[] parts = splitStringFormat(payload);
            if (parts.length < 3) {
                return; // Invalid format
            }

            String key = parts[0];
            String expectedValue = parts[1];
            String newValue = parts[2];

            // Determine the response based on conditions A and B
            char responseChar;

            // Synchronize to ensure atomicity of the CAS operation
            synchronized (this) {
                // Condition A: Does the node have a key/value pair whose key matches the requested key?
                if (keyValueStore.containsKey(key)) {
                    String currentValue = keyValueStore.get(key);
                    if (currentValue.equals(expectedValue)) {
                        keyValueStore.put(key, newValue);
                        responseChar = 'R';

                        // If this is a node address, update the nodeAddresses map too
                        if (key.startsWith("N:")) {
                            updateNodeAddress(key, newValue);
                        }
                    } else {
                        responseChar = 'N';
                    }
                }
                // Condition B: Is the node one of the three closest nodes to the requested key?
                else if (isAmongClosestNodes(key, 3)) {
                    keyValueStore.put(key, newValue);
                    responseChar = 'A';

                    // If this is a node address, update the nodeAddresses map too
                    if (key.startsWith("N:")) {
                        updateNodeAddress(key, newValue);
                    }
                }
                else {
                    responseChar = 'X';
                }
            }

            // Send the response
            String response = seqId + " D " + responseChar;
            sendPacket(response, sourceAddress, sourcePort);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleRelayRequest(String seqId, String payload, InetAddress sourceAddress, int sourcePort) {
        try {
            // Extract target node name and the message to relay
            String[] parts = splitStringFormat(payload);
            if (parts.length < 2) {
                return; // Invalid format
            }

            String targetNodeName = parts[0];
            String messageToRelay = parts[1];

            // Find the target node
            InetSocketAddress targetAddress = nodeAddresses.get(targetNodeName);
            if (targetAddress == null) {
                // Try to find the node if we don't know it
                if (!findNode(targetNodeName)) {
                    return; // Could not find the node
                }
                targetAddress = nodeAddresses.get(targetNodeName);
                if (targetAddress == null) {
                    return;
                }
            }

            // Parse the message to relay to determine if it's a request
            String[] relayMsgParts = messageToRelay.split(" ", 2);
            if (relayMsgParts.length < 2) {
                return; // Invalid format
            }

            String relaySeqId = relayMsgParts[0];
            String relayCommand = relayMsgParts[1];

            // Forward the message to the target node
            sendPacket(messageToRelay, targetAddress.getAddress(), targetAddress.getPort());

            // If it's a request message, we need to handle the response
            if (isRequestCommand(relayCommand)) {
                final String originalSeqId = seqId;
                final InetAddress replyAddress = sourceAddress;
                final int replyPort = sourcePort;

                // Set up a handler for the response
                pendingRequests.put(relaySeqId, response -> {
                    try {
                        // Forward the response back to the original sender with the original sequence ID
                        String relayResponse = originalSeqId + " " + response;
                        sendPacket(relayResponse, replyAddress, replyPort);
                    } catch (Exception e) {
                        // Silently handle errors
                    }
                });
            }
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    // Response Handlers

    private void handleNameResponse(String seqId, String payload) {
        // Extract node name from response
        String nodeName = extractString(payload);

        // Invoke the response handler if one exists for this sequence ID
        ResponseHandler handler = pendingRequests.get(seqId);
        if (handler != null) {
            handler.handleResponse(payload);
            pendingRequests.remove(seqId);
        }
    }

    private void handleNearestResponse(String seqId, String payload) {
        ResponseHandler handler = pendingRequests.get(seqId);
        if (handler != null) {
            handler.handleResponse(payload);
            pendingRequests.remove(seqId);
        }
    }

    private void handleExistsResponse(String seqId, String payload) {
        ResponseHandler handler = pendingRequests.get(seqId);
        if (handler != null) {
            handler.handleResponse(payload);
            pendingRequests.remove(seqId);
        }
    }

    private void handleReadResponse(String seqId, String payload) {
        ResponseHandler handler = pendingRequests.get(seqId);
        if (handler != null) {
            handler.handleResponse(payload);
            pendingRequests.remove(seqId);
        }
    }

    private void handleWriteResponse(String seqId, String payload) {
        ResponseHandler handler = pendingRequests.get(seqId);
        if (handler != null) {
            handler.handleResponse(payload);
            pendingRequests.remove(seqId);
        }
    }

    private void handleCASResponse(String seqId, String payload) {
        ResponseHandler handler = pendingRequests.get(seqId);
        if (handler != null) {
            handler.handleResponse(payload);
            pendingRequests.remove(seqId);
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        // Check if we know the node's address
        System.out.println("[DEBUG] Checking if node is active: " + nodeName);

        InetSocketAddress nodeAddr = nodeAddresses.get(nodeName);
        if (nodeAddr == null) {
            System.out.println("[DEBUG] Node address not found, trying to find it...");
            // Try to find the node
            if (!findNode(nodeName)) {
                System.out.println("[DEBUG] Could not find node: " + nodeName);
                return false;
            }
            nodeAddr = nodeAddresses.get(nodeName);
            if (nodeAddr == null) {
                System.out.println("[DEBUG] Still could not get node address after findNode()");
                return false;
            }
        }

        System.out.println("[DEBUG] Found node address: " + nodeAddr.getAddress().getHostAddress() + ":" + nodeAddr.getPort());

        // Send a name request to check if the node is active
        String seqId = generateSequenceId();
        String request = seqId + " G";

        System.out.println("[DEBUG] Sending activity check to node: " + seqId + " G");

        AtomicBoolean active = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        pendingRequests.put(seqId, response -> {
            System.out.println("[DEBUG] Received activity response: " + response);
            active.set(true);
            latch.countDown();
        });

        // Send the request with retries
        boolean responseReceived = false;
        for (int retry = 0; retry < MAX_RETRIES && !responseReceived; retry++) {
            if (retry > 0) {
                System.out.println("[DEBUG] Retrying activity check, attempt " + (retry + 1));
            }

            sendPacket(request, nodeAddr.getAddress(), nodeAddr.getPort());

            // Wait for the response with timeout
            responseReceived = latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        if (!responseReceived) {
            System.out.println("[DEBUG] No response received for activity check after " + MAX_RETRIES + " attempts");
        } else {
            System.out.println("[DEBUG] Node " + nodeName + " is active");
        }

        pendingRequests.remove(seqId);
        return active.get();
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'");
        }

        // Check if the node is active before adding it to the relay stack
        if (!isActive(nodeName)) {
            throw new IllegalArgumentException("Node is not active");
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
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        // Check if we have the key locally
        if (keyValueStore.containsKey(key)) {
            return true;
        }

        // Find the three closest nodes to the key
        byte[] keyHash = computeHashID(key);
        List<Map.Entry<String, String>> closestNodes = findClosestNodes(keyHash, 3);

        // Try checking existence from each node (with relaying if necessary)
        for (Map.Entry<String, String> entry : closestNodes) {
            String nodeName = entry.getKey();
            if (nodeName.equals(this.nodeName)) {
                continue; // Skip ourselves, we've already checked locally
            }

            InetSocketAddress nodeAddr = nodeAddresses.get(nodeName);
            if (nodeAddr == null) {
                continue;
            }

            // Send an exists request
            boolean exists = sendExistsRequest(key, nodeAddr);
            if (exists) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String read(String key) throws Exception {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        System.out.println("[DEBUG] Attempting to read key: " + key);

        // Check if we have the key locally
        if (keyValueStore.containsKey(key)) {
            System.out.println("[DEBUG] Found key locally: " + key + " = " + keyValueStore.get(key));
            return keyValueStore.get(key);
        }

        System.out.println("[DEBUG] Key not found locally, searching network...");

        // Calculate hash for key
        byte[] keyHash = computeHashID(key);
        String keyHashHex = bytesToHex(keyHash);
        System.out.println("[DEBUG] Key hash: " + keyHashHex);

        // Wait until we know at least one node before continuing
        if (nodeAddresses.isEmpty()) {
            System.out.println("[DEBUG] No known nodes. Waiting for contact...");

            // Try to manually discover bootstrap nodes on standard CRN ports
            try {
                for (int port = 20110; port <= 20116; port++) {
                    try {
                        System.out.println("[DEBUG] Trying to discover node at localhost:" + port);
                        InetAddress addr = InetAddress.getByName("localhost");
                        InetSocketAddress nodeAddr = new InetSocketAddress(addr, port);

                        // Send a name request to this potential node
                        String seqId = generateSequenceId();
                        String request = seqId + " G";

                        AtomicBoolean received = new AtomicBoolean(false);
                        CountDownLatch latch = new CountDownLatch(1);

                        pendingRequests.put(seqId, response -> {
                            System.out.println("[DEBUG] Received name response: " + response);
                            received.set(true);
                            latch.countDown();
                        });

                        sendPacket(request, addr, port);

                        // Wait briefly for response
                        boolean gotResponse = latch.await(2000, TimeUnit.MILLISECONDS);
                        pendingRequests.remove(seqId);

                        if (gotResponse && received.get()) {
                            System.out.println("[DEBUG] Successfully discovered node at port " + port);
                        }
                    } catch (Exception e) {
                        System.out.println("[DEBUG] Error trying port " + port + ": " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.out.println("[DEBUG] Error during bootstrap discovery: " + e.getMessage());
            }

            // Wait for nodes to contact us
            int waitTime = 0;
            while (nodeAddresses.isEmpty() && waitTime < 30000) { // Wait up to 30 seconds
                Thread.sleep(1000);
                waitTime += 1000;
                System.out.println("[DEBUG] Waiting... Current known nodes: " + nodeAddresses.size());
            }

            if (nodeAddresses.isEmpty()) {
                System.out.println("[DEBUG] No nodes contacted us after waiting. Cannot proceed.");

                // Try one more desperate approach - send a broadcast
                try {
                    System.out.println("[DEBUG] Attempting broadcast discovery");
                    // Basic broadcast to common ports
                    for (int port = 20110; port <= 20116; port++) {
                        try {
                            String seqId = generateSequenceId();
                            String request = seqId + " G";
                            sendPacket(request, InetAddress.getByName("255.255.255.255"), port);
                            System.out.println("[DEBUG] Sent broadcast to port " + port);
                        } catch (Exception e) {
                            // Ignore errors on broadcast
                        }
                    }

                    // Wait a bit more for responses
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.out.println("[DEBUG] Error during broadcast: " + e.getMessage());
                }

                // If still no nodes, give up
                if (nodeAddresses.isEmpty()) {
                    return null;
                }
            }
        }

        System.out.println("[DEBUG] Known nodes: " + nodeAddresses.keySet());
        System.out.println("[DEBUG] Nodes count: " + nodeAddresses.size());

        // Query each known node for the nearest nodes to our key
        List<Map.Entry<String, String>> closestNodes = new ArrayList<>();

        for (Map.Entry<String, InetSocketAddress> entry : nodeAddresses.entrySet()) {
            String nodeName = entry.getKey();
            InetSocketAddress nodeAddr = entry.getValue();

            System.out.println("[DEBUG] Asking " + nodeName + " at " + nodeAddr + " for nearest nodes to " + key);

            try {
                List<Map.Entry<String, String>> nearestFromNode = sendNearestRequest(keyHashHex, nodeAddr);

                if (nearestFromNode != null && !nearestFromNode.isEmpty()) {
                    System.out.println("[DEBUG] Received " + nearestFromNode.size() + " nearest nodes from " + nodeName);
                    for (Map.Entry<String, String> node : nearestFromNode) {
                        System.out.println("[DEBUG]   - " + node.getKey() + " at " + node.getValue());
                    }
                    closestNodes.addAll(nearestFromNode);

                    // Try sending a direct read request to this node as well
                    String directValue = sendReadRequest(key, nodeAddr);
                    if (directValue != null) {
                        System.out.println("[DEBUG] Successfully read value directly from " + nodeName);
                        keyValueStore.put(key, directValue);
                        return directValue;
                    }

                    break;  // We got some nodes, proceed to querying them
                } else {
                    System.out.println("[DEBUG] No nearest nodes received from " + nodeName);
                }
            } catch (Exception e) {
                System.out.println("[DEBUG] Error querying " + nodeName + " for nearest nodes: " + e.getMessage());
            }
        }

        // Deduplicate the list of nodes
        Map<String, String> uniqueNodes = new HashMap<>();
        for (Map.Entry<String, String> node : closestNodes) {
            uniqueNodes.put(node.getKey(), node.getValue());
        }

        System.out.println("[DEBUG] Found " + uniqueNodes.size() + " unique potential nodes to query");

        // Try reading from each node
        for (Map.Entry<String, String> entry : uniqueNodes.entrySet()) {
            String nodeName = entry.getKey();
            String nodeAddress = entry.getValue();

            System.out.println("[DEBUG] Attempting to read from node: " + nodeName + " at " + nodeAddress);

            try {
                // Parse the address
                String[] addrParts = nodeAddress.split(":");
                if (addrParts.length != 2) {
                    System.out.println("[DEBUG] Invalid address format: " + nodeAddress);
                    continue;
                }

                InetSocketAddress nodeAddr = new InetSocketAddress(
                        InetAddress.getByName(addrParts[0]),
                        Integer.parseInt(addrParts[1])
                );

                // Send a read request
                String value = sendReadRequest(key, nodeAddr);
                if (value != null) {
                    System.out.println("[DEBUG] Successfully read value from " + nodeName + ": " +
                            (value.length() > 30 ? value.substring(0, 30) + "..." : value));
                    // Cache the value locally
                    keyValueStore.put(key, value);
                    return value;
                } else {
                    System.out.println("[DEBUG] No value found at " + nodeName);
                }
            } catch (Exception e) {
                System.out.println("[DEBUG] Error querying node " + nodeName + ": " + e.getMessage());
            }
        }

        // If we found some nearest nodes but couldn't read from them,
        // try asking them for their nearest nodes (one level deeper)
        if (!uniqueNodes.isEmpty()) {
            System.out.println("[DEBUG] Trying secondary nearest nodes query");
            List<Map.Entry<String, String>> secondaryNodes = new ArrayList<>();

            for (Map.Entry<String, String> entry : uniqueNodes.entrySet()) {
                try {
                    String[] addrParts = entry.getValue().split(":");
                    if (addrParts.length == 2) {
                        InetSocketAddress nodeAddr = new InetSocketAddress(
                                InetAddress.getByName(addrParts[0]),
                                Integer.parseInt(addrParts[1])
                        );

                        List<Map.Entry<String, String>> moreNodes = sendNearestRequest(keyHashHex, nodeAddr);
                        if (moreNodes != null && !moreNodes.isEmpty()) {
                            secondaryNodes.addAll(moreNodes);
                        }
                    }
                } catch (Exception e) {
                    // Continue with next node
                }
            }

            // Try these secondary nodes
            Map<String, String> uniqueSecondary = new HashMap<>();
            for (Map.Entry<String, String> node : secondaryNodes) {
                uniqueSecondary.put(node.getKey(), node.getValue());
            }

            // Skip nodes we already tried
            for (String nodeName : uniqueNodes.keySet()) {
                uniqueSecondary.remove(nodeName);
            }

            System.out.println("[DEBUG] Found " + uniqueSecondary.size() + " additional nodes to try");

            // Try reading from these secondary nodes
            for (Map.Entry<String, String> entry : uniqueSecondary.entrySet()) {
                try {
                    String[] addrParts = entry.getValue().split(":");
                    if (addrParts.length == 2) {
                        InetSocketAddress nodeAddr = new InetSocketAddress(
                                InetAddress.getByName(addrParts[0]),
                                Integer.parseInt(addrParts[1])
                        );

                        String value = sendReadRequest(key, nodeAddr);
                        if (value != null) {
                            System.out.println("[DEBUG] Successfully read value from secondary node " + entry.getKey());
                            keyValueStore.put(key, value);
                            return value;
                        }
                    }
                } catch (Exception e) {
                    // Continue with next node
                }
            }
        }

        System.out.println("[DEBUG] Failed to find key: " + key);
        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        if (key == null || key.isEmpty() || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null or empty");
        }

        // If this is our own node name, update our address
        if (key.equals(nodeName)) {
            keyValueStore.put(key, value);
            return true;
        }

        // Find the three closest nodes to the key
        byte[] keyHash = computeHashID(key);
        List<Map.Entry<String, String>> closestNodes = findClosestNodes(keyHash, 3);

        boolean success = false;

        // Try writing to each of the three closest nodes
        for (Map.Entry<String, String> entry : closestNodes) {
            String nodeName = entry.getKey();

            // If this is us, write locally
            if (nodeName.equals(this.nodeName)) {
                keyValueStore.put(key, value);

                // If this is a node address, update the nodeAddresses map too
                if (key.startsWith("N:")) {
                    updateNodeAddress(key, value);
                }

                success = true;
                continue;
            }

            InetSocketAddress nodeAddr = nodeAddresses.get(nodeName);
            if (nodeAddr == null) {
                continue;
            }

            // Send a write request
            if (sendWriteRequest(key, value, nodeAddr)) {
                success = true;
            }
        }

        return success;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (key == null || key.isEmpty() || currentValue == null || newValue == null) {
            throw new IllegalArgumentException("Key, current value, and new value cannot be null or empty");
        }

        // If this is our own node name, do a local CAS
        if (key.equals(nodeName)) {
            synchronized (this) {
                String storedValue = keyValueStore.get(key);
                if (storedValue != null && storedValue.equals(currentValue)) {
                    keyValueStore.put(key, newValue);
                    return true;
                }
                return false;
            }
        }

        // Find the three closest nodes to the key
        byte[] keyHash = computeHashID(key);
        List<Map.Entry<String, String>> closestNodes = findClosestNodes(keyHash, 3);

        boolean success = false;

        // Try CAS on each of the three closest nodes
        for (Map.Entry<String, String> entry : closestNodes) {
            String nodeName = entry.getKey();

            // If this is us, do a local CAS
            if (nodeName.equals(this.nodeName)) {
                synchronized (this) {
                    String storedValue = keyValueStore.get(key);
                    if (storedValue != null && storedValue.equals(currentValue)) {
                        keyValueStore.put(key, newValue);

                        // If this is a node address, update the nodeAddresses map too
                        if (key.startsWith("N:")) {
                            updateNodeAddress(key, newValue);
                        }

                        success = true;
                    }
                }
                continue;
            }

            InetSocketAddress nodeAddr = nodeAddresses.get(nodeName);
            if (nodeAddr == null) {
                continue;
            }

            // Send a CAS request
            if (sendCASRequest(key, currentValue, newValue, nodeAddr)) {
                success = true;
            }
        }

        return success;
    }

    // Helper Methods

    private String generateSequenceId() {
        // Generate a unique sequence ID for requests
        sequenceNumber = (sequenceNumber + 1) % 100;
        return String.format("%s%02d", nodeName.substring(2), sequenceNumber);
    }

    private void sendPacket(String message, InetAddress address, int port) throws IOException {
        // Check if we need to relay the message
        if (!relayStack.isEmpty()) {
            // Get the top relay node
            String relayNodeName = relayStack.peek();
            InetSocketAddress relayAddr = nodeAddresses.get(relayNodeName);

            if (relayAddr != null) {
                // Create a relay message
                String relayMsg = generateSequenceId() + " V " + formatString(relayNodeName) + message;

                // Send to the relay node instead
                byte[] data = relayMsg.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(data, data.length, relayAddr.getAddress(), relayAddr.getPort());
                socket.send(packet);
                return;
            }
        }

        // Direct send if no relay or relay not found
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

    private String formatString(String str) {
        // Format a string as per CRN protocol: "spaceCount string "
        int spaceCount = countSpaces(str);
        return spaceCount + " " + str + " ";
    }

    private int countSpaces(String str) {
        // Count the number of spaces in a string
        int count = 0;
        for (char c : str.toCharArray()) {
            if (c == ' ') {
                count++;
            }
        }
        return count;
    }

    private String extractString(String formattedStr) {
        // Extract the string from CRN format "spaceCount string "
        String[] parts = formattedStr.trim().split(" ", 2);
        if (parts.length < 2) {
            return "";
        }
        return parts[1];
    }

    private String[] splitStringFormat(String payload) {
        // Split a payload containing multiple formatted strings
        // Example: "0 key 1 value " -> ["key", "value"]
        List<String> results = new ArrayList<>();

        System.out.println("[DEBUG] Parsing CRN format string: " + payload);

        int i = 0;
        while (i < payload.length()) {
            // Skip leading whitespace
            while (i < payload.length() && payload.charAt(i) == ' ') {
                i++;
            }

            if (i >= payload.length()) {
                break;
            }

            // Parse the space count
            int j = i;
            while (j < payload.length() && Character.isDigit(payload.charAt(j))) {
                j++;
            }

            if (j >= payload.length() || j == i) {
                System.out.println("[DEBUG] Could not parse space count at position " + i);
                break;
            }

            int spaceCount = Integer.parseInt(payload.substring(i, j));
            System.out.println("[DEBUG] Found space count: " + spaceCount);

            // Skip the space after the count
            j++;

            // Find the string
            i = j;
            int spacesFound = 0;

            // Find the end of the string (after spaceCount spaces and one more)
            while (j < payload.length()) {
                if (payload.charAt(j) == ' ') {
                    spacesFound++;
                    if (spacesFound > spaceCount) {
                        break;
                    }
                }
                j++;
            }

            if (j > i) {
                // Extract the string without the trailing space
                String extractedStr = payload.substring(i, j);
                System.out.println("[DEBUG] Extracted string: " + extractedStr);
                results.add(extractedStr);
            } else {
                System.out.println("[DEBUG] Could not extract string at position " + i);
            }

            i = j + 1;
        }

        System.out.println("[DEBUG] Parsed " + results.size() + " strings from payload");
        return results.toArray(new String[0]);
    }

    private void updateNodeAddress(String nodeName, String addressStr) {
        try {
            String[] parts = addressStr.split(":");
            if (parts.length == 2) {
                InetAddress addr = InetAddress.getByName(parts[0]);
                int port = Integer.parseInt(parts[1]);
                nodeAddresses.put(nodeName, new InetSocketAddress(addr, port));
            }
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private boolean isAmongClosestNodes(String key, int count) throws Exception {
        // Check if this node is among the count closest nodes to the key
        byte[] keyHash = computeHashID(key);
        byte[] nodeHash = computeHashID(nodeName);

        // Find all known nodes
        List<Map.Entry<String, String>> allNodes = new ArrayList<>();
        for (Map.Entry<String, String> entry : keyValueStore.entrySet()) {
            if (entry.getKey().startsWith("N:")) {
                allNodes.add(entry);
            }
        }

        // Sort by distance to the key
        final byte[] finalKeyHash = keyHash;
        allNodes.sort((e1, e2) -> {
            try {
                byte[] h1 = computeHashID(e1.getKey());
                byte[] h2 = computeHashID(e2.getKey());
                return Integer.compare(
                        calculateDistance(h1, finalKeyHash),
                        calculateDistance(h2, finalKeyHash)
                );
            } catch (Exception e) {
                return 0;
            }
        });

        // Check if our node is among the closest count nodes
        for (int i = 0; i < Math.min(count, allNodes.size()); i++) {
            if (allNodes.get(i).getKey().equals(nodeName)) {
                return true;
            }
        }

        return false;
    }

    private List<Map.Entry<String, String>> findClosestNodes(byte[] targetHash, int count) {
        // Find the count closest nodes to the target hash
        List<Map.Entry<String, String>> allNodes = new ArrayList<>();

        // Add all known nodes to the list
        for (Map.Entry<String, String> entry : keyValueStore.entrySet()) {
            if (entry.getKey().startsWith("N:")) {
                allNodes.add(entry);
            }
        }

        // Sort by distance to the target hash
        final byte[] finalTargetHash = targetHash;
        allNodes.sort((e1, e2) -> {
            try {
                byte[] h1 = computeHashID(e1.getKey());
                byte[] h2 = computeHashID(e2.getKey());
                return Integer.compare(
                        calculateDistance(h1, finalTargetHash),
                        calculateDistance(h2, finalTargetHash)
                );
            } catch (Exception e) {
                return 0;
            }
        });

        // Return the closest count nodes
        return allNodes.subList(0, Math.min(count, allNodes.size()));
    }

    /**
     * Calculate the distance between two hashIDs as defined in the CRN RFC.
     * The distance is 256 minus the number of leading bits that match.
     */
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        int matchingBits = 0;

        for (int i = 0; i < hash1.length && i < hash2.length; i++) {
            if (hash1[i] == hash2[i]) {
                matchingBits += 8;
            } else {
                // Count matching bits in this byte
                int xor = hash1[i] ^ hash2[i];
                for (int j = 7; j >= 0; j--) {
                    if ((xor & (1 << j)) == 0) {
                        matchingBits++;
                    } else {
                        break;
                    }
                }
                break;
            }
        }

        return 256 - matchingBits;
    }

    /**
     * Compute the SHA-256 hash of a key.
     */
    private byte[] computeHashID(String key) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Converts a hex string to a byte array.
     */
    private byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i+1), 16));
        }
        return data;
    }

    /**
     * Converts a byte array to a hex string.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private boolean findNode(String nodeName) throws Exception {
        // Try to find a node in the network
        byte[] nodeHash = computeHashID(nodeName);
        String nodeHashHex = bytesToHex(nodeHash);

        // Start with the nodes we know
        for (Map.Entry<String, InetSocketAddress> entry : nodeAddresses.entrySet()) {
            if (entry.getKey().equals(nodeName)) {
                return true; // We already know this node
            }

            // Send a nearest request to this node
            List<Map.Entry<String, String>> nearestNodes = sendNearestRequest(nodeHashHex, entry.getValue());

            if (nearestNodes != null) {
                for (Map.Entry<String, String> nearNode : nearestNodes) {
                    if (nearNode.getKey().equals(nodeName)) {
                        // Found the node, store its address
                        updateNodeAddress(nodeName, nearNode.getValue());
                        keyValueStore.put(nodeName, nearNode.getValue());
                        return true;
                    }

                    // Also store other discovered nodes
                    if (nearNode.getKey().startsWith("N:")) {
                        updateNodeAddress(nearNode.getKey(), nearNode.getValue());
                        keyValueStore.put(nearNode.getKey(), nearNode.getValue());
                    }
                }
            }
        }

        return false;
    }

    private boolean isRequestCommand(String command) {
        // Check if a command character represents a request message
        return "GNERV".contains(command);
    }

    // Network Request Methods

    private List<Map.Entry<String, String>> sendNearestRequest(String hashIdHex, InetSocketAddress nodeAddr) throws Exception {
        String seqId = generateSequenceId();
        String request = seqId + " N " + hashIdHex;

        System.out.println("[DEBUG] Sending nearest request to " + nodeAddr.getAddress().getHostAddress() + ":" + nodeAddr.getPort() + " with hash: " + hashIdHex);

        AtomicReference<List<Map.Entry<String, String>>> result = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(1);

        pendingRequests.put(seqId, response -> {
            try {
                System.out.println("[DEBUG] Received nearest response: " + response);

                // Parse the nearest response containing address key/value pairs
                List<Map.Entry<String, String>> nodes = new ArrayList<>();
                String[] parts = splitStringFormat(response);

                System.out.println("[DEBUG] Split response into " + parts.length + " parts");

                for (int i = 0; i < parts.length; i += 2) {
                    if (i + 1 < parts.length) {
                        String nodeName = parts[i];
                        String nodeAddress = parts[i + 1];

                        System.out.println("[DEBUG] Found node: " + nodeName + " at " + nodeAddress);

                        nodes.add(new AbstractMap.SimpleEntry<>(nodeName, nodeAddress));

                        // Store node information
                        if (nodeName.startsWith("N:")) {
                            updateNodeAddress(nodeName, nodeAddress);
                            keyValueStore.put(nodeName, nodeAddress);
                        }
                    }
                }

                result.set(nodes);
                latch.countDown();
            } catch (Exception e) {
                System.out.println("[DEBUG] Error parsing nearest response: " + e.getMessage());
                latch.countDown();
            }
        });

        // Send the request with retries
        boolean responseReceived = false;
        for (int retry = 0; retry < MAX_RETRIES && !responseReceived; retry++) {
            if (retry > 0) {
                System.out.println("[DEBUG] Retrying nearest request, attempt " + (retry + 1));
            }

            sendPacket(request, nodeAddr.getAddress(), nodeAddr.getPort());

            // Wait for the response with timeout
            responseReceived = latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        if (!responseReceived) {
            System.out.println("[DEBUG] No response received for nearest request after " + MAX_RETRIES + " attempts");
        }

        pendingRequests.remove(seqId);
        return result.get();
    }

    private boolean sendExistsRequest(String key, InetSocketAddress nodeAddr) throws Exception {
        String seqId = generateSequenceId();
        String request = seqId + " E " + formatString(key);

        AtomicBoolean result = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        pendingRequests.put(seqId, response -> {
            if (!response.isEmpty() && response.charAt(0) == 'Y') {
                result.set(true);
            }
            latch.countDown();
        });

        // Send the request with retries
        boolean responseReceived = false;
        for (int retry = 0; retry < MAX_RETRIES && !responseReceived; retry++) {
            sendPacket(request, nodeAddr.getAddress(), nodeAddr.getPort());

            // Wait for the response with timeout
            responseReceived = latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        pendingRequests.remove(seqId);
        return result.get();
    }

    private String sendReadRequest(String key, InetSocketAddress nodeAddr) throws Exception {
        String seqId = generateSequenceId();
        String request = seqId + " R " + formatString(key);

        System.out.println("[DEBUG] Sending read request to " + nodeAddr.getAddress().getHostAddress() + ":" + nodeAddr.getPort() + " for key: " + key);

        AtomicReference<String> result = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(1);

        pendingRequests.put(seqId, response -> {
            try {
                System.out.println("[DEBUG] Received read response: " + response);

                // Parse the read response - first character is Y/N/?
                if (response.startsWith("Y")) {
                    // Extract the value from the response
                    String valueStr = response.substring(2); // Skip "Y " prefix
                    System.out.println("[DEBUG] Extracted response value: " + valueStr.substring(0, Math.min(20, valueStr.length())) + "...");

                    // Extract from the CRN string format
                    String[] parts = valueStr.split(" ", 2);
                    if (parts.length > 1) {
                        result.set(parts[1]);
                    } else {
                        result.set("");
                    }
                } else if (response.startsWith("N")) {
                    System.out.println("[DEBUG] Node does not have the key but should.");
                } else if (response.startsWith("?")) {
                    System.out.println("[DEBUG] Node is not responsible for this key.");
                } else {
                    System.out.println("[DEBUG] Unknown response format: " + response);
                }

                latch.countDown();
            } catch (Exception e) {
                System.out.println("[DEBUG] Error parsing read response: " + e.getMessage());
                e.printStackTrace();
                latch.countDown();
            }
        });

        // Send the request with retries
        boolean responseReceived = false;
        for (int retry = 0; retry < MAX_RETRIES && !responseReceived; retry++) {
            if (retry > 0) {
                System.out.println("[DEBUG] Retrying read request, attempt " + (retry + 1));
            }

            sendPacket(request, nodeAddr.getAddress(), nodeAddr.getPort());

            // Wait for the response with timeout
            responseReceived = latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        if (!responseReceived) {
            System.out.println("[DEBUG] No response received for read request after " + MAX_RETRIES + " attempts");
        }

        pendingRequests.remove(seqId);
        return result.get();
    }

    private boolean sendWriteRequest(String key, String value, InetSocketAddress nodeAddr) throws Exception {
        String seqId = generateSequenceId();
        String request = seqId + " W " + formatString(key) + formatString(value);

        AtomicBoolean result = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        pendingRequests.put(seqId, response -> {
            // 'R' or 'A' response characters indicate success
            if (!response.isEmpty() && (response.charAt(0) == 'R' || response.charAt(0) == 'A')) {
                result.set(true);
            }
            latch.countDown();
        });

        // Send the request with retries
        boolean responseReceived = false;
        for (int retry = 0; retry < MAX_RETRIES && !responseReceived; retry++) {
            sendPacket(request, nodeAddr.getAddress(), nodeAddr.getPort());

            // Wait for the response with timeout
            responseReceived = latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        pendingRequests.remove(seqId);
        return result.get();
    }

    private boolean sendCASRequest(String key, String currentValue, String newValue, InetSocketAddress nodeAddr) throws Exception {
        String seqId = generateSequenceId();
        String request = seqId + " C " + formatString(key) + formatString(currentValue) + formatString(newValue);

        AtomicBoolean result = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        pendingRequests.put(seqId, response -> {
            // 'R' or 'A' response characters indicate success
            if (!response.isEmpty() && (response.charAt(0) == 'R' || response.charAt(0) == 'A')) {
                result.set(true);
            }
            latch.countDown();
        });

        // Send the request with retries
        boolean responseReceived = false;
        for (int retry = 0; retry < MAX_RETRIES && !responseReceived; retry++) {
            sendPacket(request, nodeAddr.getAddress(), nodeAddr.getPort());

            // Wait for the response with timeout
            responseReceived = latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        pendingRequests.remove(seqId);
        return result.get();
    }

    // Interface for response handling
    private interface ResponseHandler {
        void handleResponse(String response);
    }
}