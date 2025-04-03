//import java.net.InetAddress;
//import java.util.concurrent.TimeUnit;
//
//public class CRNTest {
//    private static final int TEST_TIMEOUT = 10; // seconds
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("=== CRN Protocol Debug Test Suite ===");
//
//        testBasicCommunication();
//        testRelayFunctionality();
//        testDataStorage();
//        testNetworkDiscovery();
//        testDistanceCalculation();
//    }
//
//    private static void testBasicCommunication() throws Exception {
//        System.out.println("\n=== 1. Basic Communication Test ===");
//
//        Node node1 = new Node();
//        node1.setNodeName("N:test1");
//        node1.openPort(20110);
//
//        Node node2 = new Node();
//        node2.setNodeName("N:test2");
//        node2.openPort(20111);
//
//        // Manual bootstrap
//        node1.write("N:test2", "127.0.0.1:20111");
//        node2.write("N:test1", "127.0.0.1:20110");
//
//        // Test connectivity
//        System.out.println("Node1 -> Node2: " + (node1.isActive("N:test2") ? "✅" : "❌"));
//        System.out.println("Node2 -> Node1: " + (node2.isActive("N:test1") ? "✅" : "❌"));
//
//        // Test data propagation
//        String testKey = "D:test";
//        String testValue = "Hello CRN";
//        node1.write(testKey, testValue);
//
//        System.out.println("Node1 read back: " +
//                (testValue.equals(node1.read(testKey)) ? "✅" : "❌"));
//        System.out.println("Node2 read: " +
//                (testValue.equals(node2.read(testKey)) ? "✅" : "⚠️ (may need retry)"));
//    }
//
//    private static void testRelayFunctionality() throws Exception {
//        System.out.println("\n=== 2. Relay Functionality Test ===");
//
//        Node node1 = new Node();
//        node1.setNodeName("N:relay1");
//        node1.openPort(20120);
//
//        Node node2 = new Node();
//        node2.setNodeName("N:relay2");
//        node2.openPort(20121);
//
//        Node node3 = new Node();
//        node3.setNodeName("N:relay3");
//        node3.openPort(20122);
//
//        // Set up relay chain
//        node1.pushRelay("N:relay2");
//        node2.pushRelay("N:relay3");
//
//        // Bootstrap
//        node1.write("N:relay2", "127.0.0.1:20121");
//        node2.write("N:relay3", "127.0.0.1:20122");
//
//        // Test relay
//        String relayKey = "D:relay_test";
//        String relayValue = "Relayed message";
//        node1.write(relayKey, relayValue);
//
//        TimeUnit.SECONDS.sleep(2); // Allow relay time
//
//        String result = node3.read(relayKey);
//        System.out.println("Relay result: " +
//                (relayValue.equals(result) ? "✅" :
//                        (result == null ? "❌ (check relay stack)" : "⚠️ (data corrupted)")));
//    }
//
//    private static void testDataStorage() throws Exception {
//        System.out.println("\n=== 3. Data Storage Test ===");
//
//        Node node = new Node();
//        node.setNodeName("N:storage_test");
//        node.openPort(20130);
//
//        String key = "D:storage_test";
//        String value = "Test value";
//
//        // Test write/read
//        node.write(key, value);
//        System.out.println("Basic storage: " +
//                (value.equals(node.read(key)) ? "✅" : "❌"));
//
//        // Test CAS
//        boolean casResult = node.CAS(key, value, "New value");
//        System.out.println("CAS operation: " +
//                (casResult ? "✅" : "❌ (concurrency issue)"));
//    }
//
//    private static void testNetworkDiscovery() throws Exception {
//        System.out.println("\n=== 4. Network Discovery Test ===");
//
//        Node node = new Node();
//        node.setNodeName("N:discovery_test");
//        node.openPort(20140);
//
//        System.out.println("Waiting for discoveries...");
//        node.handleIncomingMessages(TEST_TIMEOUT * 1000);
//
//        System.out.println("Discovered nodes: " + node.getKnownNodesCount());
//    }
//
//    private static void testDistanceCalculation() throws Exception {
//        System.out.println("\n=== 5. Distance Calculation Test ===");
//
//        Node node = new Node();
//        String hash1 = node.computeHashID("N:test1");
//        String hash2 = node.computeHashID("N:test2");
//
//        int distance = node.calculateDistance(hash1, hash2);
//        System.out.println("Distance between N:test1 and N:test2: " + distance);
//        System.out.println("Validation: " +
//                (distance > 0 && distance < 256 ? "✅" : "❌ (check hash calculation)"));
//    }
//}