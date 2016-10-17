package com.suman.BD.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountPair implements Writable {
	private int count;
	private String docId;
	public CountPair() {
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.count = arg0.readInt();
		this.docId = arg0.readUTF();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.count);
		arg0.writeUTF(this.docId);
	}

	public CountPair(int count, String docId) {
		this.count= count;
		this.docId = docId;
	}
	
	@Override
	public String toString() {
		return "docId=" + docId + " " + "count=" + count;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public String getDocId() {
		return docId;
	}
	public void setDocId(String docId) {
		this.docId = docId;
	}
}
