# Distributed_FSystem

1) Quick and dirty distributed file system:

QDDFS_NameServer3 is a distributed filesystem loosley based on HDFS and GFS.
The project makes use of grpc to interact with other running file systems to perform action such as replication of a file.
It has a name server which contains the metadata of all the file stores running and the data present in those file stores.
Zookeeper is used by the client, nameserver and the filestores to watch for any changes and to also choose a leader (Nameserver) from the running filestores if one nameserver fails.


2) Chain Replication

ChainRep3.1 is based on the famous research paper by Robbert van Renesse and Fred B. Schneider https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf

It makes use of grpc to interact with other replicas in the chain.
Zookeeper is used to determine the predecessor or successor or whether the current replica is a head or a tail.
