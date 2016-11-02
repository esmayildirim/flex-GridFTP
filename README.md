# flex-GridFTP
A set of interfaces to optimize data transfers with the GridFTP protocol. 
Require: globus toolkit 6.0
Capabilities: 
1. Concurrent(cc), pipelined(pp), parallel(p) transfers 
2. Multi-node concurrent transfers 
3. Replica transfers
4. Overlay transfers 
5. Manipulate different parameter settings(p,pp,cc) for partial datasets
6. Dynamically change parameter settings during the course of the transfer.

Installation Instructions:

- Install and configure globus toolkit 6.0 on every transfer node
- Launch a gridftp server
- Update the home directories and the globus library and include directories in the install.sh script
- Find examples on how to use the algorithms in concurrency_basic_algorithms.c
- Add general_utility.h to your programs
- Start using the interfaces

