package com.suman.BD.StripesMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<LongWritable, Text, Text, CustomMapWritable> {
	private HashMap<String, HashMap<String, DoubleWritable>> recordHash;
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		recordHash = new HashMap<String, HashMap<String, DoubleWritable>>();
	}
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
