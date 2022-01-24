package fileSystem;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import qddfs.FileStoreGrpc;
import qddfs.FileStoreGrpc.FileStoreBlockingStub;
import qddfs.NameServerGrpc.NameServerImplBase;
import qddfs.Qddfs16;
import qddfs.Qddfs16.BumpVersionReply;
import qddfs.Qddfs16.BumpVersionRequest;
import qddfs.Qddfs16.DeleteFileReply;
import qddfs.Qddfs16.DeleteFileRequest;
import qddfs.Qddfs16.FSEntry;
import qddfs.Qddfs16.NSAddReply;
import qddfs.Qddfs16.NSAddRequest;
import qddfs.Qddfs16.NSBeatReply;
import qddfs.Qddfs16.NSBeatRequest;
import qddfs.Qddfs16.NSCreateReply;
import qddfs.Qddfs16.NSCreateRequest;
import qddfs.Qddfs16.NSDeleteReply;
import qddfs.Qddfs16.NSDeleteRequest;
import qddfs.Qddfs16.NSListReply;
import qddfs.Qddfs16.NSListRequest;
import qddfs.Qddfs16.NSNameEntry;
import qddfs.Qddfs16.NSReadReply;
import qddfs.Qddfs16.NSReadRequest;
import qddfs.Qddfs16.NSRegisterReply;
import qddfs.Qddfs16.NSRegisterRequest;

public class NameServer extends NameServerImplBase  {

	static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private static ConcurrentHashMap<String, FileStoreMetaData> fileStoreMetadata1 = new ConcurrentHashMap<String, FileStoreMetaData>();
	private static List<String> hostPortsHeartBeatList = new ArrayList<String>();
	private static ConcurrentHashMap<String, Instant> hostPortsHeartBeatMap = new ConcurrentHashMap<String, Instant>();
	static ZooKeeper zookeeper;
	private static String zookeeper_host_port_list;
	private static String control_path;
	final static CountDownLatch connectedSignal = new CountDownLatch(1);
	private static String leaderIp = "";
	private static String leaderName = "";
	private static boolean isNameServer = false;
	private static final String myIp = "172.24.9.105:9017";
	private static Stat stat;
	private static ConcurrentHashMap<String, FileDetails> maxVersionOfFilePresent = new ConcurrentHashMap<String, FileDetails>();
	private static int maximumVersion=1;
	private static boolean canTakeRequests;
	private static int maxVersionset;
	private static int versionCounter=0;
	private static int versionToBeBumped;
	private static ConcurrentHashMap<Integer, Instant> versionsTimed = new ConcurrentHashMap<Integer, Instant>(); 
	
	public static void main(String[] args) throws Exception {
		zookeeper_host_port_list = args[0];
		control_path = args[1];
		zookeeper = new ZooKeeper(zookeeper_host_port_list, 5000, watcher);
		Thread serverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Server server = ServerBuilder.forPort(9017).addService(new NameServer()).build();

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
		createNameServer();
		stat = zookeeper.exists(control_path, true);
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				KeySetView<String, FileStoreMetaData> keySet = fileStoreMetadata1.keySet();
				keySet.removeIf(n -> !hostPortsHeartBeatList.contains(n));

			}
		};
		TimerTask task1 = new TimerTask() {
			@Override
			public void run() {
				canTakeRequests=true;

			}
		};
		
		TimerTask task2 = new TimerTask() {
			@Override
			public void run() {
				for (Entry<String, Instant> set : hostPortsHeartBeatMap.entrySet()) {
					Instant val = set.getValue();
					if(Instant.now().isAfter(val.plusSeconds(30))) {
						
						hostPortsHeartBeatList.remove(set.getKey());
					}
					
			}
			}
		};
		
		TimerTask task3 = new TimerTask() {
			@Override
			public void run() {
				
				int ver=0;
				if(versionCounter>10) {
					for(Entry<Integer, Instant> set : versionsTimed.entrySet()) {
						if(Instant.now().isAfter(set.getValue().plusSeconds(120))&& set.getKey()>ver){
							ver=set.getKey();
						}
					}
					versionToBeBumped=ver;
					bumpVersions();
					versionCounter=0;
					
				}
			}
		};
		
		
		Timer timer = new Timer();
		timer.schedule(task, 30000, 30000);
		timer.schedule(task1, 30000);
		timer.schedule(task2,30000, 5000);
		timer.schedule(task3, 120000, 60000);
		serverThread.start();
		connectedSignal.await();

	}
	
	

	private static Watcher watcher = new Watcher() {

		@Override
		public void process(WatchedEvent we) {

			if (we.getState() == KeeperState.SyncConnected) {
			} else if (we.getState() == KeeperState.Disconnected) {
				System.out.println("Client got disconnected, waiting");
			}else if(we.getState() == KeeperState.Expired) {
				System.out.println("Expired");
				connectedSignal.countDown();
				System.exit(0);
			}
			try {
				zookeeper.getChildren(control_path, true);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (we.getType() == Event.EventType.None) {
			} else if (we.getType() == Event.EventType.NodeDeleted) {

				String node = we.getPath();
				if (node.contains("leader")) {
					try {
						System.out.println("Leader changed");
						createNameServer();
					} catch (KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
				}else if(node.contains("minver")){
					
					try {
						String nodeInfo = getDataFromNode(stat, "minver");
						maximumVersion = Integer.parseInt(nodeInfo);
					} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println("deleted node" + node);

			} else if (we.getType() == Event.EventType.NodeCreated) {
				System.out.println("new node created" + we.getPath());
			} else if (we.getType() == Event.EventType.NodeChildrenChanged) {
				System.out.println("inside change" + we.getPath());

				System.out.println("children change:" + we.getPath() + we.getState());

			}

			zookeeper.exists(control_path, true, null, we);

		}
	};
	
	private static void maxVer() throws KeeperException, InterruptedException {

		Watcher watcher1 = new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				if (event.getType() == Event.EventType.NodeDeleted || event.getType() == Event.EventType.NodeCreated
						|| event.getType() == Event.EventType.NodeDataChanged) {

					String node = event.getPath();
					if (node.contains("maxver")) {
						System.out.println("leader changed");

					}
				}
			}
		};
		int ver = 0;
		try {
			// Stat stat = null;
			byte[] b = zookeeper.getData(control_path + "/" + "maxver", watcher1, stat);
			ver = zookeeper.exists(control_path + "/" + "maxver", true).getVersion();
			if (b != null) {
				String data = new String(b, "UTF-8");
				System.out.println("Maximum version :" + data);
				maximumVersion = Integer.parseInt(data);
			}
		} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
			System.out.println("minver not present currently");
		}
		maxVersionset = maximumVersion + 10;
		zookeeper.setData(control_path + "/" + "maxver", ("" + maxVersionset).getBytes(), ver);

	}

	private static void createNameServer() throws KeeperException, InterruptedException {

		String newline = System.getProperty("line.separator");

		byte[] bytes = ("172.24.9.105:9017" + newline + "Ajinkya Rajguru").getBytes();
		try {
			String leader = zookeeper.create(control_path + "/leader", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			System.out.println("I am the leader now");
			isNameServer = true;
			leaderIp = myIp;
			maxVer();
			versionToBeBumped=maximumVersion;
		} catch (Exception e1) {

			String nodeInfo = null;
			try {
				nodeInfo = getDataFromNode(stat, "leader");
			} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			if (nodeInfo != null && !nodeInfo.isEmpty() && nodeInfo.contains(System.lineSeparator())) {
				leaderIp = nodeInfo.substring(0, nodeInfo.indexOf(System.lineSeparator()));
				leaderName = nodeInfo.substring(nodeInfo.indexOf(System.lineSeparator()));
			}

			System.out.println("Current name server is:" + leaderName + " : " + leaderIp);

		}

	}

	public synchronized static String getDataFromNode(Stat stat, String path)
			throws KeeperException, InterruptedException, UnsupportedEncodingException {
		byte[] b = zookeeper.getData(control_path + "/" + path, watcher, stat);

		if (b != null) {
			String data = new String(b, "UTF-8");
			System.out.println(data);
			return data;
		}

		return null;

	}
	
	private static void bumpVersions() {
		for (Entry<String, FileStoreMetaData> set : fileStoreMetadata1.entrySet()) {
			List<String> fileList = new ArrayList<String>();
			FileStoreMetaData val = set.getValue();
			for (Entry<String, FileDetails> set1 : val.getFilesPresent().entrySet()) {
				
				if(set1.getValue().getVersion() >= maxVersionOfFilePresent.get(set1.getKey()).getVersion()) {
					
					fileList.add(set1.getKey());
		
			}
			}
			ManagedChannel channel = ManagedChannelBuilder.forTarget(set.getKey()).usePlaintext().build();
			FileStoreBlockingStub stub = FileStoreGrpc.newBlockingStub(channel);
			BumpVersionRequest bmp = BumpVersionRequest.newBuilder().addAllName(fileList).setNewVersion(versionToBeBumped).build();
			BumpVersionReply resp = stub.bumpVersion(bmp);
			
		}
		try {
			updateMinVer();
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void updateMinVer() throws KeeperException, InterruptedException {
		int ver;
		 ver = zookeeper.exists(control_path + "/" + "minver", true).getVersion();
		zookeeper.setData(control_path + "/" + "minver", (""+versionToBeBumped).getBytes(),ver);
	}

	@Override
	public void doCreate(NSCreateRequest request, StreamObserver<NSCreateReply> responseObserver) {
		NSCreateReply.Builder reply = NSCreateReply.newBuilder();
		List<String> hostPortList = new ArrayList<String>();
		int cnt=0;
		for (Entry<String, FileStoreMetaData> set : fileStoreMetadata1.entrySet()) {
			
			if(cnt<3) {

				hostPortList.add(set.getKey());
				cnt++;
			}
		}
		
		if (isNameServer && canTakeRequests) {
			if(maximumVersion> maxVersionset) {
				try {
					maxVer();
					
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			reply.addAllHostPort(hostPortList);
			reply.setVersion(maximumVersion+1);
			maximumVersion++;
			versionCounter++;
			versionsTimed.put(maximumVersion, Instant.now());
			reply.setRc(0);
		} else {
			reply.setRc(2);

		}
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();
	}

	@Override
	public void doRead(NSReadRequest request, StreamObserver<NSReadReply> responseObserver) {
		NSReadReply.Builder reply = NSReadReply.newBuilder();
		if (isNameServer && canTakeRequests) {
			String fileName = request.getName();
			List<String> hostPortList = new ArrayList();
			for (Entry<String, FileStoreMetaData> set : fileStoreMetadata1.entrySet()) {
				FileStoreMetaData val = set.getValue();
				if (val.getFilesPresent().containsKey(fileName)  
						&& !val.getFilesPresent().get(fileName).isTombStone() 
						&& val.getFilesPresent().get(fileName).getVersion() == maxVersionOfFilePresent.get(fileName).getVersion()) {

					hostPortList.add(set.getKey());

				}
			}

			if (hostPortList != null && !hostPortList.isEmpty()) {

				reply.setRc(0);
				reply.addAllHostPort(hostPortList);
			} else {

				reply.setRc(1);
			}
		} else {
			reply.setRc(2);
		}
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();

	}

	@Override
	public void doDelete(NSDeleteRequest request, StreamObserver<NSDeleteReply> responseObserver) {
		NSDeleteReply.Builder reply = NSDeleteReply.newBuilder();
		int version;
		if (isNameServer && canTakeRequests) {
			String fileNameToDelete = request.getName();
			if (maxVersionOfFilePresent.contains(fileNameToDelete)) {
				version = maxVersionOfFilePresent.get(fileNameToDelete).getVersion() + 1;
			} else {
				version = maximumVersion;
			}

			List<String> hostPortList = new ArrayList<String>();

			for (Entry<String, FileStoreMetaData> set : fileStoreMetadata1.entrySet()) {

				FileStoreMetaData val = set.getValue();
				hostPortList.add(set.getKey());
			}
			if (hostPortList != null && !hostPortList.isEmpty()) {
				for (String host : hostPortList) {
					ManagedChannel channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
					FileStoreBlockingStub stub = FileStoreGrpc.newBlockingStub(channel);
					DeleteFileRequest deleteFileReq = DeleteFileRequest.newBuilder().setName(fileNameToDelete)
							.setVersion(version).build();
					DeleteFileReply resp = stub.deleteFile(deleteFileReq);
					if (resp.getRc() == 0) {
						reply.setRc(0);
					}
				}
			}

			reply.setRc(0);

		} else {
			reply.setRc(2);
		}
		if(reply.getRc()!=0 || reply.getRc()!=0) {
			reply.setRc(1);
		}
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();
	}

	@Override
	public void list(NSListRequest request, StreamObserver<NSListReply> responseObserver) {
		NSListReply.Builder rep = NSListReply.newBuilder();
		if (isNameServer && canTakeRequests) {
			String pattern = request.getPattern();
			List<NSNameEntry> nseList = new ArrayList<Qddfs16.NSNameEntry>();
			ConcurrentHashMap<String, FileDetails> fileList = new ConcurrentHashMap<String, FileDetails>();
			for (Entry<String, FileStoreMetaData> set : fileStoreMetadata1.entrySet()) {
				FileStoreMetaData val = set.getValue();
				for (Entry<String, FileDetails> set2 : val.getFilesPresent().entrySet()) {

					if (set2.getKey().matches(pattern) && !set2.getValue().isTombStone()) {
						fileList.put(set2.getKey(), set2.getValue());
						NSNameEntry nse = NSNameEntry.newBuilder().setName(set2.getKey())
								.setVersion(set2.getValue().getVersion()).setSize(set2.getValue().getSize()).build();
						nseList.add(nse);
					}
				}

			}

			if (nseList != null && !nseList.isEmpty()) {

				rep.addAllEntries(nseList);
				rep.setRc(0);
			} else {
				rep.setRc(0);
			}
		} else {
			rep.setRc(2);
		}
		responseObserver.onNext(rep.build());
		responseObserver.onCompleted();
	}

	@Override
	public void addFileOrTombstone(NSAddRequest request, StreamObserver<NSAddReply> responseObserver) {

		FSEntry fsEntry = request.getEntry();
		String name = fsEntry.getName();
		String hostport = request.getHostPort();
		NSAddReply.Builder reply = NSAddReply.newBuilder();
		int rc = 2;
		if (isNameServer && canTakeRequests) {
			if (fileStoreMetadata1.containsKey(hostport)) {

				FileStoreMetaData fileStore = fileStoreMetadata1.get(hostport);
				ConcurrentHashMap<String, FileDetails> files = fileStore.getFilesPresent();
				if (files.containsKey(name)) {
					FileDetails file = files.get(name);
					maxVersionOfFilePresent.put(file.getFileName(), file);
					file.setVersion(fsEntry.getVersion());
					file.setTombStone(fsEntry.getIsTombstone());
					file.setSize(fsEntry.getSize());
					System.out.println("Existing file edited: " + name + "  new version: " + fsEntry.getVersion());
				} else {

					FileDetails fd = new FileDetails();
					fd.setFileName(name);
					fd.setVersion(fsEntry.getVersion());
					fd.setSize(fsEntry.getSize());
					fd.setTombStone(fsEntry.getIsTombstone());
					files.put(name, fd);
					if (maxVersionOfFilePresent.containsKey(name)) {
						if (maxVersionOfFilePresent.get(name).getVersion() < fsEntry.getVersion()) {
							maxVersionOfFilePresent.put(name, fd);
						}
					} else {
						maxVersionOfFilePresent.put(name, fd);
					}
					System.out.println("New file added: " + name);

				}

				rc = 0;
			} else {

			}
		} else {
			rc = 1;
		}
		reply.setRc(rc);
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();

	}

	@Override
	public void registerFilesAndTombstones(NSRegisterRequest request,
			StreamObserver<NSRegisterReply> responseObserver) {
		List<FSEntry> entryList = new ArrayList<>();
		entryList = request.getEntriesList();
		String hostPort = request.getHostPort();
		System.out.println("Inside register: " + hostPort);
		NSRegisterReply.Builder reply = NSRegisterReply.newBuilder();
		int rc = 2;
		if (isNameServer) {
			if (fileStoreMetadata1.containsKey(hostPort)) {
				FileStoreMetaData fsmd = fileStoreMetadata1.get(hostPort);
				ConcurrentHashMap<String, FileDetails> files = fsmd.getFilesPresent();
				for (FSEntry entry : entryList) {

					if (files.containsKey(entry.getName())) {

					}

				}

			} else {

				FileStoreMetaData fileStore = new FileStoreMetaData();
				ConcurrentHashMap<String, FileDetails> files = new ConcurrentHashMap<String, FileDetails>();

				for (FSEntry entry : entryList) {

					FileDetails fd = new FileDetails();
					fd.setVersion(entry.getVersion());
					fd.setFileName(entry.getName());
					System.out.println(fd.getFileName());
					fd.setSize(entry.getSize());
					fd.setTombStone(entry.getIsTombstone());
					System.out.println(fd);
					files.put(entry.getName(), fd);
					if (maxVersionOfFilePresent.containsKey(entry.getName())) {
						FileDetails filePresent = maxVersionOfFilePresent.get(entry.getName());
						if (filePresent.getVersion() < entry.getVersion()) {
							maxVersionOfFilePresent.put(fd.getFileName(), fd);
						}
					} else {
						maxVersionOfFilePresent.put(fd.getFileName(), fd);
					}
				}
				fileStore.setFilesPresent(files);
				fileStore.setHostport(hostPort);
				fileStore.setBytesUsed(request.getBytesUsed());
				fileStore.setBytesAvailable(request.getBytesAvailable());
				fileStoreMetadata1.put(hostPort, fileStore);
				rc = 0;
			}
			System.out.println(fileStoreMetadata1);
		} else {
			rc = 1;
		}
		reply.setRc(rc);
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();

	}

	@Override
	public void heartBeat(NSBeatRequest request, StreamObserver<NSBeatReply> responseObserver) {
		int rc =3;
		NSBeatReply.Builder reply = NSBeatReply.newBuilder();
		if(isNameServer) {
		request.getHostPort();
		request.getBytesAvailable();
		request.getBytesUsed();
		hostPortsHeartBeatList.add(request.getHostPort());
		hostPortsHeartBeatMap.put(request.getHostPort(), Instant.now());
		rc=0;
		}else {
			rc=1;
		}
		
		reply.setRc(rc);
		responseObserver.onNext(reply.build());
		responseObserver.onCompleted();
	}


}