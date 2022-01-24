package edu.sjsu.cs249.chain.replicas;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import edu.sjsu.cs249.chain.AckRequest;
import edu.sjsu.cs249.chain.AckResponse;
import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaImplBase;
import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.IncRequest;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaImplBase;
import edu.sjsu.cs249.chain.StateTransferRequest;
import edu.sjsu.cs249.chain.StateTransferResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaImplBase;
import edu.sjsu.cs249.chain.UpdateRequest;
import edu.sjsu.cs249.chain.UpdateResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class ChainReplica implements Watcher {

	private static String zookeeper_host_port_list;
	private static String control_path;
	static ZooKeeper zookeeper;
	final static CountDownLatch connectedSignal = new CountDownLatch(1);
	private final static Logger logger = Logger.getLogger("ZK");
	private static String myZnode = "/replica-";
	List<String> servers = new ArrayList<String>();
	public static ConcurrentHashMap<String, Integer> data = new ConcurrentHashMap<String, Integer>();
	private static int CurrentxId;
	private static ConcurrentHashMap<String, String> replicaDataMap = new ConcurrentHashMap<String, String>();
	private static String myNode = null;
	private static String predecessor;
	private static String successor;
	private static Stat stat;
	private static boolean isTail = false;
	private static boolean isNewTail = false;
	private static ConcurrentHashMap<Integer, UpdateRequest> sentList = new ConcurrentHashMap<Integer, UpdateRequest>();

	public static boolean isNewTail() {
		return isNewTail;
	}

	public static void setNewTail(boolean isNewTail) {
		ChainReplica.isNewTail = isNewTail;
	}

	public static synchronized boolean isTail() {
		return isTail;
	}

	public static synchronized void setTail(boolean isTail) {
		ChainReplica.isTail = isTail;
	}

	public static synchronized boolean isHead() {
		return isHead;
	}

	public static synchronized void setHead(boolean isHead) {
		ChainReplica.isHead = isHead;
	}

	private static boolean isHead = false;
	private static Watcher watcher = new Watcher() {

		@Override
		public void process(WatchedEvent we) {
			try {
				zookeeper.getChildren(control_path, true);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (we.getType() == Event.EventType.None) {
				logger.info("Connection expired");
			} else if (we.getType() == Event.EventType.NodeDeleted) {

				String node = we.getPath();
				System.out.println("deleted node" + node);
				if (node.contains(myNode)) {
					System.out.println("My node got removed");
				}

			} else if (we.getType() == Event.EventType.NodeCreated) {
				System.out.println("new node created" + we.getPath());
			} else if (we.getType() == Event.EventType.NodeChildrenChanged) {
				System.out.println("inside change" + we.getPath());

				System.out.println("children change:" + we.getPath() + we.getState());

			}
			try {
				updateReplicas();
			} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			zookeeper.exists(control_path, true, null, we);

		}

	};

	public static ConcurrentHashMap<String, Integer> getData() {
		return data;
	}

	public static void setData(ConcurrentHashMap<String, Integer> data) {
		ChainReplica.data = data;
	}

	private static synchronized int getXId() {

		return CurrentxId;
	}

	private static synchronized void setXId(int xId) {

		CurrentxId = xId;
	}

	private static synchronized void setUpdatedData(ConcurrentHashMap<String, Integer> map) {
		setData(map);

	}

	private static synchronized String getMySuccessor() {
		return replicaDataMap.get(successor);

	}

	private static synchronized String getMyPredecessor() {
		return replicaDataMap.get(predecessor);

	}

	public static void main(String args[]) throws IOException, InterruptedException, KeeperException {

		if (args.length < 2) {
			System.err.println("Need two argumenst to access the path ");
			System.exit(2);
		}

		zookeeper_host_port_list = args[0];
		control_path = args[1];
		//int port = Integer.parseInt(args[2]);
		Thread serverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Server server = ServerBuilder.forPort(9010).addService(new HeadChainReplicaService())
						.addService(new TailChainReplicaService()).addService(new ReplicaService()).build();

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
		zookeeper = new ZooKeeper(zookeeper_host_port_list, 2000, watcher);

		createNode();
		stat = zookeeper.exists(control_path, true);
		updateReplicas();
		if (stat != null) {
			logger.info("Node exists");
			stat.getPzxid();

			List<String> list = zookeeper.getChildren(control_path, true);
			System.out.println(list);
			for (String path : list) {
				getDataFromNode(stat, path);
			}

		}
		serverThread.start();
		connectedSignal.await();

	}

	public static void createNode() throws IOException, KeeperException, InterruptedException {
		byte[] bytes = Files.readAllBytes(
				Paths.get("/Users/ajinkyarajguru/eclipse-workspace/ChainReplication/src/main/resources/data"));

		myNode = zookeeper.create(control_path + myZnode, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
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

	static synchronized void updateReplicas()
			throws KeeperException, InterruptedException, UnsupportedEncodingException {
		stat = zookeeper.exists(myNode, true);
		if (stat == null) {

			System.out.println("my node is deleted");
			return;
		}
		String myNodeName = myNode.substring(myNode.lastIndexOf("r"), myNode.length());

		if (stat != null) {

			List<String> list = null;
			try {
				list = zookeeper.getChildren(control_path, true);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String max = Collections.max(list);
			String min = Collections.min(list);
			Collections.sort(list);
			System.out.println("after sort");
			int myIndex = list.indexOf(myNodeName);
			if (myNodeName.equals(max) && myNodeName.equals(min)) {
				isHead = true;
				isTail = true;
				successor = null;
				predecessor = null;
			} else if (myNodeName.equals(max)) {
				isTail = true;
				isHead = false;
				predecessor = list.get(myIndex - 1);
			} else if (myNodeName.equals(min)) {
				isTail = false;
				isHead = true;
				successor = list.get(1);
			} else {
				isTail = false;
				isHead = false;
				predecessor = list.get(myIndex - 1);
				successor = list.get(myIndex + 1);
			}
			System.out.println(list);
			for (String node : list) {
				String nodeInfo = null;
				try {
					nodeInfo = getDataFromNode(stat, node);
				} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (nodeInfo != null && !nodeInfo.isEmpty() && nodeInfo.contains(System.lineSeparator())) {
					replicaDataMap.put(node, nodeInfo.substring(0, nodeInfo.indexOf(System.lineSeparator())));
					System.out.println(replicaDataMap.get(node));
				}
			}
			if (successor != null) {
				stateTransferToSuccessor();
			}
			if (isTail && sentList != null && !sentList.isEmpty()) {
				checkSentListAndSendRemainingAcks();
			}
			if (successor != null && predecessor != null) {
				System.out.println("Successor is: " + replicaDataMap.get(successor));
				System.out.println("predecessor is:" + replicaDataMap.get(predecessor));
			}
		}
		if (successor != null) {
			stateTransferToSuccessor();
		}

	}

	static int sendUpdateToSuccessor(String key, int newVal, int xId) {
		int respResult = 0;
		ConcurrentHashMap<String, Integer> mapUp = new ConcurrentHashMap<>();
		mapUp = getData();
		System.out.println("in updateToSuccessor");
		String successor = getMySuccessor();
		int currentXid = getXId();

		if (!isHead) {
			if (currentXid < xId) {
				mapUp.put(key, newVal);
				System.out.print("going to update");
				setUpdatedData(mapUp);
				setXId(xId);
			} else {
				return 3;
			}
		}
		ExecutorService executorService = Executors.newCachedThreadPool();
		Callable<Integer> callableTask = () -> {
			System.out.print("inside execute");
			ManagedChannel channel = ManagedChannelBuilder.forTarget(successor).usePlaintext().build();
			System.out.println(getXId());
			System.out.println("mySuccessor" + getMySuccessor());
			System.out.println("new val" + newVal);
			ReplicaBlockingStub stub = ReplicaGrpc.newBlockingStub(channel);
			UpdateRequest request = UpdateRequest.newBuilder().setKey(key).setNewValue(newVal).setXid(xId).build();
			sentList.put(xId, request);

			System.out.println("xID" + xId);
			System.out.println("sending update");
			UpdateResponse resp = null;
			try {
				resp = stub.update(request);
			} catch (Exception e) {
				System.out.println("Exception occured while sending update to predecessor");

			}

			if (resp.getRc() == 0) {
				System.out.println("sending 0, ack received fir xID"+xId);
				return resp.getRc();
			}

			return stateTransferToSuccessor();

		};

		Future<Integer> result = executorService.submit(callableTask);
		if (result.isDone()) {
			try {
				respResult = result.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return respResult;
	}

	static int stateTransferToSuccessor() {
		if (!isTail) {
			ManagedChannel channel = ManagedChannelBuilder.forTarget(getMySuccessor()).usePlaintext().build();
			ReplicaBlockingStub stub = ReplicaGrpc.newBlockingStub(channel);

			List<UpdateRequest> upReq = new ArrayList();
			for (int i : sentList.keySet()) {
				upReq.add(sentList.get(i));
			}
			StateTransferRequest stateTransferReq = StateTransferRequest.newBuilder().putAllState(getData())
					.setXid(getXId()).addAllSent(upReq).build(); // setSent(getXId(), a).build();

			StateTransferResponse stateTransferResp = stub.stateTransfer(stateTransferReq);
			System.out.println("state transfer to"+ getMySuccessor());
			if (stateTransferResp.getRc() == 0) {

				return 0;
			}
		}

		return 1;

	}

	static synchronized void sendAckToPredecessor(int xId) {
		
			
			
			
				editSentList(xId);
				System.out.println("deleted:" + xId);
				if (isHead) {
					System.out.println("I am head going out of acks");
					synchronized(sentList) {
						if(!sentList.contains(xId)){
							sentList.notify();
						}
					}
					return;
				}
				ManagedChannel channel = ManagedChannelBuilder.forTarget(getMyPredecessor()).usePlaintext().build();
				ReplicaBlockingStub stub = ReplicaGrpc.newBlockingStub(channel);
				AckRequest AckReq = AckRequest.newBuilder().setXid(xId).build();
				try {
					AckResponse ackResp = stub.ack(AckReq);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Exception occured while sending an Ack, trying again" + xId);

				}
				System.out.println("Ack send for" + xId);
				System.out.println("acknowledgement sent to predecessor");
			}
		
		
	

	private static synchronized void editSentList(int xId) {
		if (sentList.containsKey(xId)) {
			sentList.remove(xId);

		} else {

			System.out.println("tried deleting the key again, the key is not present");
		}
	}

	static int headInc(String key, int val) {
		System.out.println("inside head inc");

		int newValue = 0;
		if (checkIfKeyIsPresent(key)) {
			newValue = IncData(key, val);

		} else {

			newValue = IncNewData(key, val);
		}
		int xid=incXid();
		ExecutorService executorService = Executors.newCachedThreadPool();
		Callable<Integer> callableTask = () -> {
			int response;
			if (isTail) {
				response = 0;
			} else {

				response = sendUpdateToSuccessor(key, data.get(key), getXId());
			}
			
			
			return response;
		};
		int respResult = 0;
		Future<Integer> result = executorService.submit(callableTask);
		if (result.isDone()) {
			try {
				respResult = result.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		synchronized(sentList) {
			System.out.print("in wait");
			try {
				sentList.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("sent list edited and receieved ack" + val);
		return respResult;

	}

	private static synchronized int incXid() {
		CurrentxId++;
		return CurrentxId;

	}

	private static synchronized boolean checkIfKeyIsPresent(String key) {
		return getData().containsKey(key);
	}

	private static synchronized int IncData(String key, int inc) {

		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap();
		int value = getData().get(key);
		int newVal = value + inc;
		map.put(key, newVal);
		setUpdatedData(map);

		return newVal;
	}

	private static synchronized int IncNewData(String key, int inc) {

		updateData(key, inc, 0);
		data.put(key, inc);

		return inc;
	}

	@Override
	public void process(WatchedEvent we) {
		if (we.getType() == Event.EventType.NodeDeleted) {

			String node = we.getPath();
			System.out.println("deleted node niche wala" + node);

		}

	}

	private static synchronized void updateData(String key, int newValue, int xId) {

		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
		map = getData();
		map.put(key, newValue);
		setXId(xId);
		setUpdatedData(map);

	}

	private static synchronized int getdataInformation(String key) {
		int val = 0;
		if (data.containsKey(key)) {
			val = data.get(key);
		}
		return val;
	}

	private static synchronized void checkSentListAndSendRemainingAcks() {

		List<Integer> xIds = new ArrayList<Integer>();
		if (sentList != null && !sentList.isEmpty()) {
			System.out.println("sent list not empty, sending acks");
			for (int xId : sentList.keySet()) {

				if (xId <= getXId()) {

					xIds.add(xId);

				}
			}
			for (int delxId : xIds) {
				System.out.println("ack for Xid:" + delxId);
				sendAckToPredecessor(delxId);
			}
		}
	}

	public static class HeadChainReplicaService extends HeadChainReplicaImplBase {

		@Override
		public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {

			System.out.println("inside head chain replica");
			int inc = request.getIncValue();
			String key = request.getKey();
			HeadResponse.Builder resp = HeadResponse.newBuilder();

			if (ChainReplica.isHead()) {
				if (headInc(key, inc) == 0) {
					resp.setRc(0);
					System.out.println("increment by:" + inc + " for key:" + key + " data looks like:" + getData());
				}
			} else {
				resp.setRc(1);
			}
			System.out.println("sending response for "+ getXId());

			responseObserver.onNext(resp.build());
			responseObserver.onCompleted();

		}

	}

	public static class ReplicaService extends ReplicaImplBase {

		@Override
		public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
			System.out.println("in update");
			String key = request.getKey();
			int newValue = request.getNewValue();
			int xId = request.getXid();
			UpdateResponse.Builder resp = UpdateResponse.newBuilder();
			if (!isTail()) {
				int r = sendUpdateToSuccessor(key, newValue, xId);
				resp.setRc(r);
			} else if (isTail) {
				updateData(key, newValue, xId);
				resp.setRc(0);
				sendAckToPredecessor(xId);
				System.out.println("sent ack if tail");

			}

			responseObserver.onNext(resp.build());
			responseObserver.onCompleted();
		}

		@Override
		public void stateTransfer(StateTransferRequest request,
				StreamObserver<StateTransferResponse> responseObserver) {
			request.getSentList();
			System.out.println();
			List<UpdateRequest> a = request.getSentList();
			for (UpdateRequest request1 : a) {
				sentList.put(request1.getXid(), request1);
			}
			ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>(request.getStateMap());
			setUpdatedData(map);
			int recentXid = request.getXid();
			setXId(recentXid);
			StateTransferResponse.Builder resp = StateTransferResponse.newBuilder();
			resp.setRc(0);
			if (isTail) {
				checkSentListAndSendRemainingAcks();
			}
			System.out.println(data);
			System.out.println(sentList);
			System.out.println(getXId());
			if (!isTail) {
				stateTransferToSuccessor();
			}
			responseObserver.onNext(resp.build());
			responseObserver.onCompleted();
		}

		@Override
		public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {

			int xId = request.getXid();
			sendAckToPredecessor(xId);
			AckResponse.Builder res = AckResponse.newBuilder();
			responseObserver.onNext(res.build());
			responseObserver.onCompleted();

		}

	}

	public static class TailChainReplicaService extends TailChainReplicaImplBase {

		@Override
		public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {

			GetResponse.Builder resp = GetResponse.newBuilder();

			if (!isTail()) {
				resp.setRc(1);
			} else {

				int value = getdataInformation(request.getKey());

				resp.setRc(0);
				resp.setValue(value);
			}
			responseObserver.onNext(resp.build());
			responseObserver.onCompleted();

		}

	}

}
