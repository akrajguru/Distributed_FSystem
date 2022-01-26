package fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import qddfs.FileStoreGrpc;
import qddfs.FileStoreGrpc.FileStoreImplBase;
import qddfs.FileStoreGrpc.FileStoreStub;
import qddfs.NameServerGrpc;
import qddfs.NameServerGrpc.NameServerBlockingStub;
import qddfs.Qddfs16.BumpVersionReply;
import qddfs.Qddfs16.BumpVersionRequest;
import qddfs.Qddfs16.CopyFileReply;
import qddfs.Qddfs16.CopyFileRequest;
import qddfs.Qddfs16.CreateFileReply;
import qddfs.Qddfs16.CreateFileRequest;
import qddfs.Qddfs16.DeleteFileReply;
import qddfs.Qddfs16.DeleteFileRequest;
import qddfs.Qddfs16.ErrorResult;
import qddfs.Qddfs16.FSEntry;
import qddfs.Qddfs16.FileClose;
import qddfs.Qddfs16.FileCreate;
import qddfs.Qddfs16.FileData;
import qddfs.Qddfs16.FileRead;
import qddfs.Qddfs16.ListReply;
import qddfs.Qddfs16.ListRequest;
import qddfs.Qddfs16.NSAddReply;
import qddfs.Qddfs16.NSAddRequest;
import qddfs.Qddfs16.NSBeatReply;
import qddfs.Qddfs16.NSBeatRequest;
import qddfs.Qddfs16.NSRegisterReply;
import qddfs.Qddfs16.NSRegisterRequest;
import qddfs.Qddfs16.OpenResult;
import qddfs.Qddfs16.ReadFileReply;
import qddfs.Qddfs16.ReadFileRequest;
import qddfs.Qddfs16.ReadRequest;

public class FNSystem extends FileStoreImplBase {
	
	static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private static final String location = "/Users/ajinkyarajguru/eclipse-workspace/QDDFS_NameServer3/src/main/resources/FileStore/";
	private static final String templocation = "/Users/ajinkyarajguru/eclipse-workspace/QDDFS_NameServer3/src/main/resources/Temp/";
	private static final String deleteLocation = "/Users/ajinkyarajguru/eclipse-workspace/QDDFS_NameServer3/src/main/resources/Trash/";
	private static final String metadataFile="/Users/ajinkyarajguru/eclipse-workspace/QDDFS_NameServer3/src/main/resources/FileStore/metadata.json";
	private static final String tombStoneFile="/Users/ajinkyarajguru/eclipse-workspace/QDDFS_NameServer3/src/main/resources/FileStore/tombstone.json";
	private static final String fileSizeMeta="/Users/ajinkyarajguru/eclipse-workspace/QDDFS_NameServer3/src/main/resources/FileStore/fileSize.json";
	final static CountDownLatch connectedSignal = new CountDownLatch(1);
	private static ConcurrentHashMap<String, Integer> existingFiles = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, Integer> deletedFiles = new ConcurrentHashMap<>();
	private static int minVersion;
	private static  ConcurrentHashMap<String, Long> fileSize = new ConcurrentHashMap<>();
	private static Stat stat;
	static ZooKeeper zookeeper;
	private static String zookeeper_host_port_list;
	private static String control_path;
	private static String myNode;
	static CountDownLatch countdown;
	private static final String myIp= "myIp";
	private static String leaderIp = "";
	private static String leaderName = "";
	private static boolean isNameServer=false;
	private static ManagedChannel channel;
	private static NameServerBlockingStub stub;
	static Timer timer ;
	static TimerTask task ;
	public static void main(String[] args) throws IOException, InterruptedException {
		existingFiles= getMetaDataOfExistingFiles();
		deletedFiles=getTombstoneOfDeletedFiles();
		fileSize=getMetaDataOfFileSize();
		logger.log(Level.INFO,"Inside 9015");
		zookeeper_host_port_list = args[0];
		control_path = args[1];
		Thread serverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Server server = ServerBuilder.forPort(9015).addService(new FNSystem()).build();

				try {
					server.start();
				} catch (IOException e) {
					e.printStackTrace();
				}

				System.out.println("Server started");
				try {
					server.awaitTermination();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		zookeeper = new ZooKeeper(zookeeper_host_port_list, 5000, new Watcher() {
			@Override
			public void process(WatchedEvent we) {

				// do error handling
				if (we.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}else if(we.getState() == KeeperState.Disconnected) {
					System.out.println("");
				}
				if(we.getType() == Event.EventType.NodeChildrenChanged) {
						System.out.println("Node children changed");
				}
				try {
					zookeeper.getChildren(control_path, true);
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		serverThread.start();
		try {
			try {
				
			} catch (Exception e) {
				
				e.printStackTrace();
			}
		} catch (Exception e1) {
			
			e1.printStackTrace();
		}
		
		try {
			createNode();
			identifyNameServer();
			garbageCollectOlderVersions();
		} catch (IOException | KeeperException | InterruptedException e) {
			
			e.printStackTrace();
		}
		 try {
			timer = new Timer();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 task = new TimerTask() {
			@Override
			public void run() {
				if(isNameServer) {
				hearBeatToNameServer();
				}
				
			}
		};
		TimerTask task1 = new TimerTask() {
			@Override
			public void run() {
				if(!isNameServer) {
					try {
						identifyNameServer();
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
		};
		
		timer.schedule(task, 0, 15000);
		Timer tmer2 = new Timer();
		tmer2.schedule(task1, 500, 10000 );
		
		
		connectedSignal.await();
		
	}
	public static void createNode() throws IOException, KeeperException, InterruptedException {
		String newline = System.getProperty("line.separator");
       
		byte[] bytes = ("myIp" +newline+"Ajinkya Rajguru").getBytes();

		 myNode = zookeeper.create(control_path + "/AJFS", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
	}


	
	private static void garbageCollectOlderVersions() {
		Watcher watcherMinVer = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				
				if (event.getType() == Event.EventType.NodeDeleted || event.getType() == Event.EventType.NodeCreated
						|| event.getType() == Event.EventType.NodeDataChanged) {

					String node = event.getPath();
					if(node.contains("minver")) {
						
						garbageCollectOlderVersions();
						System.out.println("min version changed");
					}

				
				}

				
			}
		};
		System.out.println();
		try {
			//Stat stat = null;
			byte[] b = zookeeper.getData(control_path + "/" + "minver", watcherMinVer, stat);

			if (b != null) {
				String data = new String(b, "UTF-8");
				System.out.println(data);
				minVersion=Integer.parseInt(data);
			}
		} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
			System.out.println("minver not present currently");
		}
		List<String> listToDelete = new ArrayList<String>();
		for (Entry<String, Integer> set : existingFiles.entrySet()) {

			Integer version = set.getValue();
			if (version < minVersion) {
				listToDelete.add(set.getKey());
			}
		}
		for (Entry<String, Integer> set : deletedFiles.entrySet()) {

			Integer version = set.getValue();
			if (version < minVersion) {
				listToDelete.add(set.getKey());
			}
		}

		for (String removeFile : listToDelete) {

			if (existingFiles.containsKey(removeFile)) {
				existingFiles.remove(removeFile);
				String tempfileName=getFileNameToStoreAfterEdition(removeFile);
				Path path = Paths.get(location+tempfileName);
				try {
					Files.deleteIfExists(path);
					System.out.println("Garbage collection: " + removeFile);
				} catch (IOException e) {
					System.out.println("exception occured in garbage collcetion while deleting a file");
					e.printStackTrace();
				}
				
			} else if (deletedFiles.containsKey(removeFile)) {
				System.out.println("Garbage collection for tombStone: " + removeFile);
				deletedFiles.remove(removeFile);
			}

		}
		setMetadata(existingFiles);
		setTombStone(deletedFiles);
		

	}
	
	private static void identifyNameServer() throws KeeperException, InterruptedException {
		
		Watcher watcherLeader = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				
				if (event.getType() == Event.EventType.NodeDeleted || event.getType() == Event.EventType.NodeCreated
						|| event.getType() == Event.EventType.NodeDataChanged) {

					String node = event.getPath();
					if(node.contains("leader")) {
						try {
							System.out.println("leader changed");
							identifyNameServer();
						} catch (KeeperException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.println("deleted node" + node);

				
				}

				
			}
		};
		
		String nodeInfo = null;
		try {
			System.out.println("In identify leader");
			byte[] b = zookeeper.getData(control_path + "/" + "leader", watcherLeader, stat);

			if (b != null) {
				String data = new String(b, "UTF-8");
				System.out.println(data);
				nodeInfo=data;
			}
		} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
			System.out.println("leader not present currently");
			isNameServer=false;
			System.out.println("sleeping for 5 secs");
			Thread.sleep(10000);
			identifyNameServer();
		}
		if (nodeInfo != null && !nodeInfo.isEmpty() && nodeInfo.contains(System.lineSeparator())) {
			leaderIp=nodeInfo.substring(0, nodeInfo.indexOf(System.lineSeparator()));
			leaderName=nodeInfo.substring(nodeInfo.indexOf(System.lineSeparator()));
			System.out.println("leader is:" +leaderName+ " IP:"+ leaderIp);
			channel = ManagedChannelBuilder.forTarget(leaderIp).usePlaintext().build();
			 stub = NameServerGrpc.newBlockingStub(channel);
			 isNameServer=true;
			registerMetaDataWithNameServer();
		}
	
		
		 
	}

	private static void registerMetaDataWithNameServer() {

		List<FSEntry> fsEntryList = new ArrayList<>();
		for (Entry<String, Integer> entry : existingFiles.entrySet()) {
			Long ver=0L;
				ver=fileSize.get(entry.getKey());
			FSEntry fsEntry = FSEntry.newBuilder().setName(entry.getKey()).setVersion(entry.getValue())
					.setIsTombstone(false).setSize(ver).build();
			fsEntryList.add(fsEntry);
		}
		for (Entry<String, Integer> entry : deletedFiles.entrySet()) {

			FSEntry fsEntry = FSEntry.newBuilder().setName(entry.getKey()).setVersion(entry.getValue())
					.setIsTombstone(true).setSize(0L).build();
			fsEntryList.add(fsEntry);
		}

		NSRegisterRequest register = NSRegisterRequest.newBuilder().addAllEntries(fsEntryList).setBytesUsed(1234567)
				.setBytesAvailable(7654321).setHostPort(myIp).build();
		NSRegisterReply resp = stub.registerFilesAndTombstones(register);
		int rc = resp.getRc();
		System.out.println("Rc receievd after sending all the meta data to NameServer:" + rc);
	}
	
	private static void addFileOrTombstone(String fileName, int version, Boolean isTombStone, long l) {
		
		FSEntry fsEntry = FSEntry.newBuilder().setName(fileName).setVersion(version)
				.setIsTombstone(isTombStone).setSize(l).build();
		NSAddRequest request = NSAddRequest.newBuilder().setEntry(fsEntry).setHostPort(myIp).build();	
		NSAddReply resp = stub.addFileOrTombstone(request);
		int rc=3;
		if(resp!=null) {
		 rc = resp.getRc();
		}
		System.out.println("Rc receievd after updating meta data to NameServer for file:"+fileName+" RC: " + rc);
	}
	
	private static void hearBeatToNameServer() {
		NSBeatRequest request = NSBeatRequest.newBuilder().setBytesAvailable(654321).setBytesUsed(123456).setHostPort(myIp).build();
		
		try{NSBeatReply resp = stub.heartBeat(request);
		if(resp.getRc()==0) {
			logger.log(Level.INFO, "Heartbeat sent to the name server");
		}else {
			logger.log(Level.SEVERE, "Heartbeat sent to an inactive server");
		}
		}catch(Exception e) {
			System.out.println("heartbeat could not be sent as leader is changing");
			//timer.schedule(task, 0, 15000);
		}
		
		
	}
	
	

	

	private static ConcurrentHashMap<String, Integer> getMetaDataOfExistingFiles() {
		ConcurrentHashMap<String, Integer> metadata = new ConcurrentHashMap<String, Integer>();
		ObjectMapper mapper = new ObjectMapper();
		 
		 
        /**
         * Read JSON from a file into a Map
         */
		File file = new File(
    			metadataFile);
		if(file.length()<=0) {
			logger.log(Level.INFO, "Meta data file is empty");
			return metadata;
		}
        try {
        	metadata = mapper.readValue(file, new TypeReference<ConcurrentHashMap<String, Integer>>() {
            });
        }catch(Exception io) {
        	io.printStackTrace();
        }
		
		return metadata;
	}
	
	private static void setMetadata(ConcurrentHashMap<String, Integer> map) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			
            String json = objectMapper.writeValueAsString(map);
            System.out.println(json);
            FileWriter file = null;
           
			try {
				
				file = new FileWriter(metadataFile);
				objectMapper.writeValue(file, map);
				
			} catch (IOException e) {
				
				e.printStackTrace();
			}
            
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e1) {
			
			e1.printStackTrace();
		}
		
		
	}
	
	private static void setfileSizeMeta() {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			
            String json = objectMapper.writeValueAsString(fileSize);
            System.out.println(json);
            FileWriter file = null;
           
			try {
				
				file = new FileWriter(fileSizeMeta);
				objectMapper.writeValue(file, fileSize);
				
			} catch (IOException e) {
				
				e.printStackTrace();
			}
            
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e1) {
			
			e1.printStackTrace();
		}
		
		
	}
	
	private static ConcurrentHashMap<String, Long> getMetaDataOfFileSize() {
		ConcurrentHashMap<String, Long> fileSizeMetadata = new ConcurrentHashMap<String, Long>();
		ObjectMapper mapper = new ObjectMapper();
		 
		 
        /**
         * Read JSON from a file into a Map
         */
		File file = new File(
    			fileSizeMeta);
		if(file.length()<=0) {
			logger.log(Level.INFO, "Meta data file is empty");
			return fileSizeMetadata;
		}
        try {
        	fileSizeMetadata = mapper.readValue(file, new TypeReference<ConcurrentHashMap<String, Long>>() {
            });
        }catch(Exception io) {
        	io.printStackTrace();
        }
		
		return fileSizeMetadata;
	}
	private static void setTombStone(ConcurrentHashMap<String, Integer> deletedFiles2) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			
            String json = objectMapper.writeValueAsString(deletedFiles2);
            System.out.println(json);
            FileWriter file = null;
           
			try {
				
				file = new FileWriter(tombStoneFile);
				objectMapper.writeValue(file, deletedFiles2);
				
			} catch (IOException e) {
				
				e.printStackTrace();
			}
            
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e1) {
			
			e1.printStackTrace();
		}
		
	}
	private static ConcurrentHashMap<String, Integer> getTombstoneOfDeletedFiles() {
		ConcurrentHashMap<String, Integer> metadata = new ConcurrentHashMap<String, Integer>();
		ObjectMapper mapper = new ObjectMapper();
		 
        /**
         * Read JSON from a file into a Map
         */
		File file = new File(
    			tombStoneFile);
		if(file.length()<=0) {
			logger.log(Level.INFO, "Meta data file is empty");
			return metadata;
		}
        try {
        	metadata = mapper.readValue(file, new TypeReference<ConcurrentHashMap<String, Integer>>() {
            });
        }catch(Exception io) {
        	io.printStackTrace();
        }
		
		return metadata;
	}
	
	private static String getFileNameToStoreAfterEdition(String fileName) {

		System.out.println("File name received:" + fileName);
		String editedFileName;
		editedFileName = fileName.replaceAll(":", "");
		editedFileName = editedFileName.replaceAll("/", "");
		System.out.println("File name stored:" + editedFileName);
		return editedFileName;

	}

	@Override
	public StreamObserver<CreateFileRequest> createFile(StreamObserver<CreateFileReply> responseObserver) {

		return new StreamObserver<CreateFileRequest>() {
			String tempfileName;
			int version;
			RandomAccessFile writer;
			FileChannel channel;
			List<String> chainList = new ArrayList<String>();
			String file;
			int rc=3;
			String message;
			FileCreate create;
			FileStoreStub stub;
			ManagedChannel grpcchannel;
			StreamObserver<CreateFileRequest> streamOb;
			String mySuccesor;
			StreamObserver<CreateFileReply> sR;
			Path filePath;
			CountDownLatch latch;
			String name;
			Path path;
			Boolean onComp=false;

			@Override
			public void onNext(CreateFileRequest value) {
				 List<String > listToSend = new ArrayList<String>();
				 
				 if (value.hasCreate()) {
					 
					 	create = value.getCreate();
					 	name = create.getName();
					 	tempfileName=getFileNameToStoreAfterEdition(name);
						version = create.getVersion();
						file = templocation + tempfileName;
						 path = Paths.get(file);
						chainList = create.getChainList();
						System.out.println("Inside create: tempfilename:"+ tempfileName+" file name being used: "+name);
						
						if (chainList != null && !chainList.isEmpty() && !(chainList.size()==1 && chainList.get(0).equals(myIp))) {
							 latch = new CountDownLatch(1);
							mySuccesor = chainList.get(0);
							
							for(String a: chainList) {
								if(!a.equals(mySuccesor)) {
								listToSend.add(a);
								System.out.println("ips of replicas"+ a);
								}
							}
							listToSend.remove(mySuccesor);
							System.out.print(listToSend);
							grpcchannel = ManagedChannelBuilder.forTarget(mySuccesor).usePlaintext().build();
							stub = FileStoreGrpc.newStub(grpcchannel);
							sR = sendToReplica();
							streamOb = stub.createFile(sR);
						}
					if ((existingFiles.get(name)==null || existingFiles.get(name)<=version)
							&& (deletedFiles.get(name)==null || deletedFiles.get(name)<version)) {
						try {
							Files.deleteIfExists(path);
							 filePath = Files.createFile(path);
							if (mySuccesor != null) {
								FileCreate create = FileCreate.newBuilder().addAllChain(listToSend).setName(name)
										.setVersion(version).build();
								streamOb.onNext(CreateFileRequest.newBuilder().setCreate(create).build());
							}
							logger.log(Level.INFO, "Created a file at: " + filePath);
						} catch (IOException e1) {
							logger.log(Level.SEVERE, "Error creating a file");
							e1.printStackTrace();
						}
//						if(deletedFiles.get(name)!=null && deletedFiles.get(name) < version) {
//							
//							deletedFiles.remove(name);
//							setTombStone(deletedFiles);
//							
//						}
					} else if (existingFiles.get(name)!=null && existingFiles.get(name) > version) {

						rc = 1;
						logger.log(Level.WARNING, "File: " + name + "version received too old /n Current version: "
								+ existingFiles.get(name) + "received version: " + version);
						message="File too old";
						responseObserver.onNext(CreateFileReply.newBuilder().setRc(rc).setMessage(message).build());
						onComp=true;
						responseObserver.onCompleted();
						

					} else if(deletedFiles.get(name)!=null && deletedFiles.get(name) > version) {
						rc = 1;
						logger.log(Level.WARNING, "File: " + name + "file already deleted. version received too old /n deleted version: "
								+ deletedFiles.get(name) + "received version: " + version);
						message="File with same or greater version already deleted";
						responseObserver.onNext(CreateFileReply.newBuilder().setRc(rc).setMessage(message).build());
						onComp=true;
						responseObserver.onCompleted();
						
						
					}
				}
				if (value.hasData()) {
					try {
						
							FileData fileData = value.getData();
							ByteString data = fileData.getData();
							long offset = fileData.getOffset();
							writer = new RandomAccessFile(file, "rw");
							channel = writer.getChannel();
							ByteBuffer b = ByteBuffer.wrap(data.toByteArray());
							channel.write(b, offset);
							if(mySuccesor!=null) {
							b.flip();
							FileData dataToBeSent = FileData.newBuilder().setData(ByteString.copyFrom(b)).setOffset(offset)
									.build();
							streamOb.onNext(CreateFileRequest.newBuilder().setData(dataToBeSent).build());
							}
							logger.log(Level.INFO, "Data stored to file " + name);
							
							

					} catch (FileNotFoundException e) {
						logger.log(Level.SEVERE, "File not found: " + name);
						e.printStackTrace();
						responseObserver.onError(e);
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Error storing data to file " + name);
						e.printStackTrace();
						responseObserver.onError(e);
					}
				}
				if (value.hasClose()) {
					
					if(mySuccesor!=null) {
					FileClose fileClose = FileClose.newBuilder().build();
					streamOb.onNext(CreateFileRequest.newBuilder().setClose(fileClose).build());
					streamOb.onCompleted();
					}else {
						rc=0;
					}
					logger.log(Level.INFO, "File closed");
				}

			}

		

			@Override
			public void onError(Throwable t) {
				rc = 2;
				message = "error occured while storing file";
				logger.log(Level.SEVERE, "Error occured while storing " + name);
				try {
					Files.deleteIfExists(path);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				logger.log(Level.INFO, "File removed from temp folder because of error: " + name);
				t.printStackTrace();
				try {
					if (channel != null && writer != null) {
						channel.close();
						writer.close();
					}
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Error caused while closing the writer channel");
					e.printStackTrace();
				}

			}

			@Override
			public void onCompleted() {
				logger.log(Level.INFO, " Inside On completed of my server");
				try {
					if(mySuccesor!=null) {
					logger.log(Level.INFO, "Latch before wait");
					latch.await();
					logger.log(Level.INFO, "Latch after wait");
					}
				} catch (InterruptedException e1) {
					
					e1.printStackTrace();
				}

				logger.log(Level.INFO, "Main rc received: " + rc);

				if (rc == 0) {
					
					existingFiles.put(name, version);
					setMetadata(existingFiles);
					setfileSizeMeta();
					moveFileTomainLocation(file, location + tempfileName);
					message = "File stored successfully";
					if(deletedFiles.get(name)!=null && deletedFiles.get(name) < version) {
						
						deletedFiles.remove(name);
						setTombStone(deletedFiles);
						
						
					}
					try {
						fileSize.put(name, writer.length());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						addFileOrTombstone(name,version, false, writer.length());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					responseObserver.onNext(CreateFileReply.newBuilder().setRc(rc).setMessage(message).build());
				} else if(rc==2){

					try {
						Files.deleteIfExists(path);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					message = "Error occured while sending file to the replica";
					logger.log(Level.INFO,
							"File could not be stored because of some error caused while sending it to a replica: "
									+ name);
					if(!onComp) {
					responseObserver.onNext(CreateFileReply.newBuilder().setRc(rc).setMessage(message).build());
					}

				}else if(rc==1) {
					
					try {
						Files.deleteIfExists(path);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					message = "Version too old";
					logger.log(Level.INFO,
							"File could not be stored because version too old: "
									+ name);
					if(!onComp) {
					responseObserver.onNext(CreateFileReply.newBuilder().setRc(rc).setMessage(message).build());
					}
				}

				try {
					if(channel!=null && writer!=null) {
					channel.close();
					writer.close();
					}
					

				} catch (IOException e) {
					logger.log(Level.SEVERE, "Error caused while closing the writer channel");
					e.printStackTrace();
				}
				if(!onComp) {
				responseObserver.onCompleted();
				}
			}

			private void moveFileTomainLocation(String source, String target) {
				try {
					Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
				} catch (IOException e) {
					logger.log(Level.INFO, "File not found to delete");
					//e.printStackTrace();
				}
				
			}



			private StreamObserver<CreateFileReply> sendToReplica() {
				return new StreamObserver<CreateFileReply>() {
					
					int rcFromSuccessor;
					@Override
					public void onNext(CreateFileReply rvalue) {
						
						logger.log(Level.INFO, "Inside onNext of the client reply");
						logger.log(Level.INFO,
								"sent request to replica Inside on next and received a response " + rvalue.getRc());
						rcFromSuccessor=rvalue.getRc();

					}

					@Override
					public void onError(Throwable t) {
						logger.log(Level.SEVERE, "In onError of clients reply");
						logger.log(Level.SEVERE, "Error occured while sending the file to the replica");
						rcFromSuccessor=2;
						message="error while sending message to replica";
						latch.countDown();
						t.printStackTrace();
						
					}

					@Override
					public void onCompleted() {
						logger.log(Level.INFO, "Inside onCompleted of clients reply");
						logger.log(Level.INFO, "Main rc received from the successor: "+ rcFromSuccessor  );
						if(rcFromSuccessor==0) {
							rc=0;
							logger.log(Level.INFO, "File sent to the replica");
							
						}else if(rcFromSuccessor==1) {
							rc=1;
						}else {
							rc=2;
						}
						//waiter=false;
						logger.log(Level.INFO, "Latch countdown");
						latch.countDown();
						
					}
				};
			}
		};

	}

	@Override
	public StreamObserver<ReadFileRequest> readFile(StreamObserver<ReadFileReply> responseObserver) {

		return new StreamObserver<ReadFileRequest>() {
			String fileNameToread;
			String fileNameWithOutReg;
			FileChannel channel;
			RandomAccessFile reader;
			ByteBuffer bf;
			long offset;

			@Override
			public void onNext(ReadFileRequest value) {
				OpenResult open = null;
				if (value.hasRead()) {
					logger.log(Level.INFO,"Inside onNext --> has read");
					FileRead read = value.getRead();
					fileNameToread = read.getName();
					System.out.print("Before in read"+fileNameToread);
					fileNameWithOutReg=getFileNameToStoreAfterEdition(fileNameToread);
					System.out.print("After in read"+fileNameToread);
					if (existingFiles.containsKey(fileNameToread)) {
						
						System.out.println("FIle name to be read found: "+fileNameToread);
						String file = location + fileNameWithOutReg;
						try {
							reader = new RandomAccessFile(file, "rw");
							try {
								open=OpenResult.newBuilder().setRc(0).setVersion(existingFiles.get(fileNameToread)).setLength(reader.length()).build();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							 responseObserver.onNext(ReadFileReply.newBuilder().setOpen(open).build());
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}else if(deletedFiles.containsKey(fileNameToread)) {
						System.out.print(" In read File has been deleted "+fileNameToread);
						open = OpenResult.newBuilder().setRc(1).setVersion(deletedFiles.get(fileNameToread)).setError("File has been deleted").setLength(-1).build();
						responseObserver.onNext(ReadFileReply.newBuilder().setOpen(open).build());
						responseObserver.onCompleted();
					}else {
						System.out.print(" In read File is not present "+fileNameToread);
						open = OpenResult.newBuilder().setRc(1).setVersion(-1).setError("File is not present").setLength(-1).build();
						responseObserver.onNext(ReadFileReply.newBuilder().setOpen(open).build());
						responseObserver.onCompleted();
					}
					
					try {
						if(reader!=null&& reader.length()<=0) {
							open = OpenResult.newBuilder().setRc(2).setVersion(-1).setError("File has no contents").setLength(-1).build();
							ErrorResult error = ErrorResult.newBuilder().setError("File has not contents").setRc(2).build();
							responseObserver.onNext(ReadFileReply.newBuilder().setOpen(open).setError(error).build());
						}
						 
					} catch (IOException e) {
						e.printStackTrace();
					};

				}
				if (value.hasReq()) {
					ReadRequest request = value.getReq();
					int length = request.getLength();
					offset = request.getOffset();
					
					if (existingFiles.containsKey(fileNameToread)) {
						
						
						try {
							
							channel = reader.getChannel();
							bf = ByteBuffer.allocate(length);
							
							
							int b =channel.read(bf, offset);

							bf.flip();
							
							ByteString byteStr = ByteString.copyFrom(bf);
							FileData data = FileData.newBuilder().setData(byteStr).setOffset(offset)
									.build();
							responseObserver.onNext(ReadFileReply.newBuilder().setData(data).build());
							
							if (b != length) {
								System.out.println("last chunk of file sent");
								responseObserver.onCompleted();
								return;
							}
							
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				
				if(value.hasClose()) {
					logger.log(Level.INFO,"Inside onNext --> has close");
					responseObserver.onCompleted();
				}
			}

			@Override
			public void onError(Throwable t) {
				logger.log(Level.SEVERE, "Inside OnError of read request");
				responseObserver.onCompleted();
			}

			@Override
			public void onCompleted() {
				try {
					if (channel != null && reader != null) {
						channel.close();
						reader.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				logger.log(Level.INFO, "Inside onCompleted in read");
				responseObserver.onCompleted();
			}
		};
	}

	@Override
	public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileReply> responseObserver) {

		String name = request.getName();
		int version = request.getVersion();
		int rc = 0;
		String message = "";
		DeleteFileReply.Builder reply = DeleteFileReply.newBuilder();
		logger.log(Level.INFO, "Inside delete");
		System.out.print("Before in delete "+name);
		String fileNameWithoutReg;
		fileNameWithoutReg=getFileNameToStoreAfterEdition(name);
		System.out.print("After in delete "+name);
		if (existingFiles.containsKey(name) || deletedFiles.containsKey(name)) {
			if (existingFiles.containsKey(name) && existingFiles.get(name) > version) {
				rc = 1;
				message="greater version of file exists";

			}else if(deletedFiles.containsKey(name) && deletedFiles.get(name)>version) {
				rc=1;
				message="greater version of deleted file exists";
			}else {
			
				String file = location + fileNameWithoutReg;

				try {
					// Moving my file to a trash folder
					Files.move(Paths.get(file), Paths.get(deleteLocation+"/"+fileNameWithoutReg), StandardCopyOption.REPLACE_EXISTING);
					
					deletedFiles.put(name, version);
					existingFiles.remove(name);
					setMetadata(existingFiles);
					setTombStone(deletedFiles);
					fileSize.put(name, 0L);
					setfileSizeMeta();
					addFileOrTombstone(name,version, true,0l);
					rc = 0;
					message="file deleted successfully";
				} catch (IOException e) {
					
					e.printStackTrace();
				}

			}
			
			
		}else {
			rc=0;
			deletedFiles.put(name, version);
			fileSize.put(name, 0L);
			setTombStone(deletedFiles);
			setfileSizeMeta();
			addFileOrTombstone(name,version, true,0l);
		}
		reply.setMessage(message);
		reply.setRc(rc);
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();
	}

	@Override
	public void list(ListRequest request, StreamObserver<ListReply> responseObserver) {
		ListReply.Builder reply = ListReply.newBuilder();
		List<FSEntry> fsEntryList = new ArrayList<>();
		for (Entry<String, Integer> entry : existingFiles.entrySet()) {
			Long ver=0L;
			if(fileSize.contains(entry.getKey())) {
				ver=fileSize.get(entry.getKey());
			}else {
				ver=500L;
			}
			FSEntry fsEntry = FSEntry.newBuilder().setName(entry.getKey()).setVersion(entry.getValue())
					.setIsTombstone(false).setSize(ver).build();
			fsEntryList.add(fsEntry);
		}
		for (Entry<String, Integer> entry : deletedFiles.entrySet()) {

			FSEntry fsEntry = FSEntry.newBuilder().setName(entry.getKey()).setVersion(entry.getValue())
					.setIsTombstone(true).setSize(0).build();
			fsEntryList.add(fsEntry);
		}
		reply.addAllEntries(fsEntryList);
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();
		
		
	}

	@Override
	public void copyFile(CopyFileRequest request, StreamObserver<CopyFileReply> responseObserver) {
		// TODO Auto-generated method stub
		super.copyFile(request, responseObserver);
	}

	@Override
	public void bumpVersion(BumpVersionRequest request, StreamObserver<BumpVersionReply> responseObserver) {
		
		List<String> filesToBump= request.getNameList();
		int version=request.getNewVersion();
		for (String fileName : filesToBump) {
			if (existingFiles.contains(fileName) && existingFiles.get(fileName)<version) {
				existingFiles.put(fileName, version);
				addFileOrTombstone(fileName, version, false,fileSize.get(fileName));
			}
		}
		setMetadata(existingFiles);
		
		BumpVersionReply.Builder rep = BumpVersionReply.newBuilder();
		responseObserver.onNext(rep.build());
		responseObserver.onCompleted();
	}

	
	

}
