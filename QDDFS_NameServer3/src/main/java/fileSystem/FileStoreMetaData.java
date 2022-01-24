package fileSystem;

import java.util.concurrent.ConcurrentHashMap;

public class FileStoreMetaData {
	
	private  String hostport;
	private long bytesUsed;
	private long bytesAvailable;
	private  ConcurrentHashMap<String, FileDetails> filesPresent;
	public String getHostport() {
		return hostport;
	}
	public void setHostport(String hostport) {
		this.hostport = hostport;
	}
	public ConcurrentHashMap<String, FileDetails> getFilesPresent() {
		return filesPresent;
	}
	public void setFilesPresent(ConcurrentHashMap<String, FileDetails> filesPresent) {
		this.filesPresent = filesPresent;
	}
	public long getBytesUsed() {
		return bytesUsed;
	}
	public void setBytesUsed(long bytesUsed) {
		this.bytesUsed = bytesUsed;
	}
	public long getBytesAvailable() {
		return bytesAvailable;
	}
	public void setBytesAvailable(long bytesAvailable) {
		this.bytesAvailable = bytesAvailable;
	}
	
	
}
