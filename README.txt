Build Instructions
==================

1. Compile all Java files in the directory:

javac *.java

2. Run the AzureLabTest program to test the node functionality:

java AzureLabTest

Working Functionality
=====================
LocalTest:
- Fully Functional
- All Tests have been passed
- There are a few debugging messages within but all files are running

- Node initialization and configuration
- Basic UDP messaging functionality
- Passive node discovery
- Name requests/responses
- Active discovery attempts on common CRN ports (20110-20116)
- Nearest request/response implementation for node discovery
- Comprehensive debug logging
- Hash calculation for DHT functionality
- Read, Write, CAS operations are implemented correctly
- Relay mechanism for forwarding messages

Known Issues
=====================

1. Cannot find poem verses: 
The main issue appears to be with the key/value store functionality. The node receives the nearest request and responds with the local node, but cannot find nodes that actually contain the poem data. This may be because:
   - The node is only finding itself and not discovering the test nodes on the network
   - The bootstrap mechanism might not be connecting to the correct network
   - The node is properly sending nearest requests, but cannot find the actual data nodes

2. Limited network connectivity:
The debug logs show that the node can only see itself (N:amid.olundegun@city.ac.uk) and cannot discover other nodes on the network. When asking for nearest nodes to the poem hash, it only returns itself.

3. IP Address mismatch: 
The node is using two different IPs in communication:
   - Local connections on 127.0.0.1
   - The stored address is 10.216.35.6
   This could be causing connection issues when trying to reach other nodes.

Investigation and Debugging
The debug logs show that:
1. The node successfully receives and processes a name request
2. When asked for nearest nodes to the poem hash, it only returns itself
3. When reading the key "D:jabberwocky0", the response is "N" (node doesn't have it but should)
4. Secondary nearest nodes query also only returns the same node
