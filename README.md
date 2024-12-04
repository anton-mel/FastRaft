CPSC 426 - Fall 2024 - Final Project

## Team Members

- Bryan Sebaraj, bws26, bryan.sebaraj@yale.edu
- Anton Melnychuk, am3785, anton.melnychuk@yale.edu

---

## Milestones

1. **Transition to gRPC**  
   - Refactor the Vanilla Raft implementation to use gRPC for inter-node communication.  
   - Define and implement `.proto` files for all necessary Raft RPCs (AppendEntries, RequestVote, etc.).  
   - Establish robust error-handling and retry mechanisms to handle real-world networking issues.  
   - Benchmark the gRPC-based implementation for correctness and compare latency with the simulated network.  

2. **FastRaft Prototype and Comparison**  
   - Implement the optimizations described in the FastRaft paper, focusing on reducing election time and improving log replication throughput.  
   - Rewrite existing test cases to validate FastRaft's correctness and performance improvements.  
   - Develop additional test cases for scenarios unique to FastRaft, such as optimized quorum handling.  

3. **AWS EC2 Deployment and Testing**  
   - Deploy the gRPC-based Vanilla Raft and FastRaft implementations across EC2 clusters in geographically diverse regions.  
   - Measure performance metrics such as latency, throughput, and fault recovery times under varying workloads.  
   - Conduct comparative analysis to quantify the improvements offered by FastRaft over Vanilla Raft.  

4. **Final Deliverables**  
   - Submit a detailed report including:  
     - Overview of both implementations.  
     - Performance benchmarking results.  
     - Analysis of trade-offs between Vanilla Raft and FastRaft.  
   - Provide a demo showcasing both implementations running on AWS.
     