package com.suman.BD.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
/*
 * @author: Suman Lama
 * @namespace Pair
 * @memberOf com.suman.BD.pair
 * @description: 
 * Pairs implementation with key and value
 * @property {String} key, key of the pair
 * @property {value} value, value of the pair
 * 
 * implements Writable and WritableComparable to make it writable at the end.
 * */
public class Pair implements Writable, WritableComparable<Pair> {
	private String key;
	private String value;
	public Pair() {
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.key = arg0.readUTF();
		this.value = arg0.readUTF();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.key);
		arg0.writeUTF(this.value);
	}
	@Override
	public int compareTo(Pair p) {
		int comparekeyResult = this.key.compareTo(p.key);
		if(comparekeyResult == 0) {
			if(p.value.equals("*")) {
				return 1;
			}
			return this.value.compareTo(p.value);
		}
		else return comparekeyResult;
	}
	@Override
	public boolean equals(Object o) {
		if(o == null) return false;
		if(!(o instanceof Pair)) return false;
		Pair p = (Pair) o;
		if(p.key.equals(this.key) && p.value.equals(this.value)) return true;
		return false;
	}
	@Override 
	public int hashCode() {
		return this.key.hashCode() ^ this.value.hashCode();
	}
	public Pair(String key, String value) {
		this.key = key;
		this.value = value;
	}
	@Override
	public String toString() {
		return key + " " + value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	
}
