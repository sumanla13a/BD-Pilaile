package com.suman.BD.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * @author Suman
 * @description Generic pair class.
 * @generics - Use wrapper classes as key and value
 */

public class PairGenericTest<K, V> implements Writable, WritableComparable<PairGenericTest<K,V>> {
	private K key;
	private V value;
	public PairGenericTest() {
	}
	
	public PairGenericTest(K key, V value) {
		this.key = key;
		this.value = value;
	}
	
	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == null) return false;
		if(!(o instanceof PairGenericTest)) return false;
		PairGenericTest<K, V> p = (PairGenericTest<K, V>) o;
		if(p.key.equals(this.key) && p.value.equals(this.value)) return true;
		return false;
	}
	
	@Override 
	public int hashCode() {
		return this.key.hashCode() ^ this.value.hashCode();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(PairGenericTest o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
