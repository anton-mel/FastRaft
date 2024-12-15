## Fast Raft

For our final project for CPSC 426 (Fall 2024), we implemented FastRaft, a variant of the popular
Raft consensus algorithm. Fast Raft optimizes the log replication process to introducing a different
replication strategy to reduce the average round trip latency between leaders and followers,
enabling higher throughput under heavy loads (and lower network failure conditions). We also
containerized the Raft and FastRaft nodes and deployed them onto an EKS cluster using Terraform,
conducting various correctness and performance benchmarks.

### Team Members

- Bryan Sebaraj, bws26, bryan.sebaraj@yale.edu
- Anton Melnychuk, am3785, anton.melnychuk@yale.edu

### Building and Running

#### Building docker images:

- Install `docker` or Docker Desktop
- Raft Node: `cd raft && docker buildx build . --platform linux/amd64 -f Dockerfile -t <raft_image_repo> && docker push <raft_image_repo>`
- FastRaft Node: `cd fastraft && docker buildx build . --platform linux/amd64 -f Dockerfile.fast -t
<fastraft_image_repo> && docker push <fastraft_image_repo>`
- Load Tester: `cd raft && docker buildx build . --platform linux/amd64 -f Dockerfile.load -t
<loadtester_image_repo> && docker push <loadtester_image_repo>`

#### Deploying to EKS

- Install `terraform`, `awscli`, and `kubectl`
- Configure your AWS credentials and kubernetes config (create cluster role bindings, etc).
- `cd raft/nodegroup`
- Modify `main.tf` to point to image repositories and have correct AWS URLs
- Run: `terraform init && terraform plan && terraform apply`
  - Note: Remember to update IAM roles on AWS (relatively straightforward on the console/website)

#### Testing

- To run test scripts:
  - `kubectl get pods` to get the pod name for the raftload pod
  - `kubectl exec -it <raftload_pod_name> -- sh`
  - `./app "ME=${SELF_IP}:5000" "PEERS=${PEERS_IP}"` to run the load tester on the raft cluster
    - change `${PEERS_IP}` to the addresses of the fast raft nodes if testing fast raft
- To change network characterictics of the cluster:
  - `kubectl exec -it <raft_pod_name> -- sh` to attach to a raft or fast raft node
  - `tc qdisc add dev eth0 root netem delay 100ms` to add a 100ms delay to the network
  - `tc qdisc add dev eth0 root netem loss 1%` to add a random packet loss of 1%
  - `tc qdisc del dev eth0 root` to remove the delay or packet loss
  - mirror commands to other nodes in cluster as desired

### Demonstration

The demonstration video can be found via Google Drive [here](https://drive.google.com/file/d/1CAnlGilz4y45UXrxEveRO-Res6b4B9j5/view?usp=sharing).

### Group Work

- Bryan SebaRaj: Containerized Raft/FastRaft nodes, deployed Raft/FastRaft onto EKS via Terraform,
  added tests and conducted performance/correctness experiments, debugged FastRaft, wrote the README, and filmed the
  demonstration video. Contributed equally to write-up.
- Anton Melnychuk: Swapped out network layer for gRPC, bootstrapped Raft, and implemented the
  FastRaft optimizations. Contributed equally to write-up.

