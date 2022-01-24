package fileSystem;

import java.util.List;

public class FileDetails {
	
	private  String fileName;
	private  int version;
	private  boolean isTombStone;
	private  long size;
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public boolean isTombStone() {
		return isTombStone;
	}
	public void setTombStone(boolean isTombStone) {
		this.isTombStone = isTombStone;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	
	

}
