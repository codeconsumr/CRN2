import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class CRNTest {
    public static void main(String[] args) throws Exception {
        // Test 1: Basic node communication
        testBasicCommunication();

        // Test 2: Relay functionality
        testRelayFunctionality();

        // Test 3: Data storage and retrieval
        testDataStorage();
    }

    private static void testBasicCommunication() throws Exception {
        System.out.println("=== Testing Basic Communication ===");

        Node node1 = new Node();
        node1.setNodeName("N:test1");
        node1.openPort(20110);

        Node node2 = new Node();
        node2.setNodeName("N:test2");
        node2.openPort(20111);

        // Make nodes aware of each other
        node1.write("N:test2", "127.0.0.1:20111");
        node2.write("N:test1", "127.0.0.1:20110");

        // Test connectivity
        System.out.println("Node1 active: " + node1.isActive("N:test2"));
        System.out.println("Node2 active: " + node2.isActive("N:test1"));

        // Test data storage
        String testKey = "D:test";
        String testValue = "Hello CRN";
        node1.write(testKey, testValue);

        // Verify data can be read from both nodes
        System.out.println("Node1 read: " + node1.read(testKey));
        System.out.println("Node2 read: " + node2.read(testKey));
    }

    private static void testRelayFunctionality() throws Exception {
        System.out.println("\n=== Testing Relay Functionality ===");

        Node node1 = new Node();
        node1.setNodeName("N:relay1");
        node1.openPort(20120);

        Node node2 = new Node();
        node2.setNodeName("N:relay2");
        node2.openPort(20121);

        Node node3 = new Node();
        node3.setNodeName("N:relay3");
        node3.openPort(20122);

        // Set up relay chain: node1 -> node2 -> node3
        node1.pushRelay("N:relay2");
        node2.pushRelay("N:relay3");

        // Make nodes aware of each other
        node1.write("N:relay2", "127.0.0.1:20121");
        node2.write("N:relay3", "127.0.0.1:20122");

        // Test relayed write
        String relayKey = "D:relay_test";
        String relayValue = "Relayed message";
        node1.write(relayKey, relayValue);

        // Verify data at end of relay chain
        TimeUnit.SECONDS.sleep(1); // Allow time for relay
        System.out.println("Relayed read: " + node3.read(relayKey));
    }

    private static void testDataStorage() throws Exception {
        System.out.println("\n=== Testing Data Storage ===");

        Node node1 = new Node();
        node1.setNodeName("N:storage1");
        node1.openPort(20130);

        Node node2 = new Node();
        node2.setNodeName("N:storage2");
        node2.openPort(20131);

        // Make nodes aware of each other
        node1.write("N:storage2", "127.0.0.1:20131");
        node2.write("N:storage1", "127.0.0.1:20130");

        // Test CAS operation
        String casKey = "D:cas_test";
        String initialValue = "Initial";
        String newValue = "Updated";

        node1.write(casKey, initialValue);
        System.out.println("Initial value: " + node1.read(casKey));

        boolean casResult = node1.CAS(casKey, initialValue, newValue);
        System.out.println("CAS result: " + casResult);
        System.out.println("Updated value: " + node1.read(casKey));

        // Test failed CAS
        casResult = node1.CAS(casKey, "WrongValue", "ShouldNotUpdate");
        System.out.println("Failed CAS result: " + casResult);
        System.out.println("Value remains: " + node1.read(casKey));
    }
}