package edu.sjsu.cs249.chain.client;

import java.util.List;

import edu.sjsu.cs249.chain.HeadChainReplicaGrpc;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.IncRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Chain_client {
	
	private static List<String> ports;
	
	public static void main(String args[]) {



		System.out.println("calling replica");
		replica();

	}
	private static void replica() {

		ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8085").usePlaintext().build();
		//ReplicaBlockingStub stub = ReplicaGrpc.newBlockingStub(channel);
		
		HeadChainReplicaBlockingStub stub = HeadChainReplicaGrpc.newBlockingStub(channel);

		IncRequest req = IncRequest.newBuilder().setKey("A").setIncValue(5).build();
		HeadResponse resp= stub.increment(req);
		
//		UpdateRequest request = UpdateRequest.newBuilder().setKey(null).setNewValue(0).setXid(0).build();
//		//StateTransferRequest stateTransferReq = StateTransferRequest.newBuilder().setSent(0, request)
//		AckRequest AckReq = AckRequest.newBuilder().setXid(0).build();
//
//		UpdateResponse resp = stub.update(request);
//		StateTransferResponse stateTransferResp = stub.stateTransfer(null);
//		AckResponse AckResp = stub.ack(AckReq);
	}
	
	
}
