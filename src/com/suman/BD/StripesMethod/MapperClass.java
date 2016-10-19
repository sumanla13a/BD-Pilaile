package com.suman.BD.StripesMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * @author: Suman Lama
 * @namespace MapperClass
 * @memberOf com.suman.BD.StripesMethod
 * @description: 
 * Mapper class
 * */
public class MapperClass extends Mapper<LongWritable, Text, Text, CustomMapWritable> {
	private HashMap<String, HashMap<String, DoubleWritable>> recordHash;
	/*
	 * @memberOf com.suman.BD.StripesMethod.MapperClass
	 * @description: 
	 * @method setup, initial setup of the mapper. Creates hashmap
	 * @params {Context} context
	 * */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		recordHash = new HashMap<String, HashMap<String, DoubleWritable>>();
	}
	/*
	 * @memberOf com.suman.BD.StripesMethod.MapperClass
	 * @method map, main mapper method. This is called recursively with each input lines from files
	 * 
	 * @params {LongWritable} key, the bytecode for the current line
	 * @params {Text} value, the current line
	 * @params {Context} context
	 * @throws IOException, InterruptedException
	 * 
	 * @description: 
	 * Counts occurence of each item after a certain item (say A) till the same item (A) re-occurs
	 * Uses Stripes method i.e each relation is stored in an associative array
	 * */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] allKeys = line.split(" ");
		int len = allKeys.length;
		for(int i = 0; i<len; i++) {
			if(i != 0) {
				String[] neighbors = Arrays.copyOfRange(allKeys, i+1, len);
				HashMap<String, DoubleWritable> neighborHash = new HashMap<String, DoubleWritable>();
				for(String entryNeighbor : neighbors) {
					if(!entryNeighbor.equals(allKeys[i])) {
						if(!neighborHash.containsKey(entryNeighbor)) {
							neighborHash.put(entryNeighbor, new DoubleWritable(1));
						}
						else {
							DoubleWritable currentValue = (DoubleWritable)neighborHash.get(entryNeighbor);
							neighborHash.put(entryNeighbor, new DoubleWritable(currentValue.get() + 1));
						}
					} else {
						break;
					}
				}
				if(!recordHash.containsKey(allKeys[i]))	recordHash.put(allKeys[i], neighborHash);
				else {
					for(String entryKey : neighborHash.keySet()) {
						if(recordHash.get(allKeys[i]).containsKey(entryKey)) {
							DoubleWritable hashEntryValue = (DoubleWritable)recordHash.get(allKeys[i]).get(entryKey);
							DoubleWritable neighborEntryValue = (DoubleWritable)neighborHash.get(entryKey);
							recordHash.get(allKeys[i]).put(entryKey, new DoubleWritable(hashEntryValue.get() + neighborEntryValue.get()));
						} else {
							recordHash.get(allKeys[i]).put(entryKey, neighborHash.get(entryKey));
						}
					}
				}
			}
		}
	}
	/*
	 * @memberOf com.suman.BD.StripesMethod.MapperClass 
	 * @method cleanup, end process of the mapping
	 * @params {Context} context
	 * @description
	 * Writes the output of mapper
	 * */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(String entry : recordHash.keySet()) {
			CustomMapWritable mapWritable = new CustomMapWritable();
			for(String eachEntry : recordHash.get(entry).keySet()) {
				mapWritable.put(new Text(eachEntry), recordHash.get(entry).get(eachEntry));
			}
			context.write(new Text(entry), mapWritable);
		}
		recordHash.clear();
	}
}
