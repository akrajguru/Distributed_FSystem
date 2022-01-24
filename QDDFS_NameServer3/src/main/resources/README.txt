In this project implements a distributed file system which makes use of GRPC framework to interact with other  file stores. The file store
class has been implemented in the FNSystem.java file present inside fileSystem package. To manage all the file stores a name server or a master has been
implemented similar to the one on google file system or loosely based on the  namenode in the hadoop file system. 
The GRPC proto file is present in the resources folder (qddfs-1-6.proto).
A zookeeper API is also used to keep track of the filestores and keep a watch on all the nodes.