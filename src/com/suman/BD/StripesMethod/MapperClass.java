package com.suman.BD.StripesMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<LongWritable, Text, Text, CustomMapWritable> {
	private HashMap<String, CustomMapWritable> recordHash;
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		recordHash = new HashMap<String, CustomMapWritable>();
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] allKeys = line.split(" ");
		int len = allKeys.length;
		for(int i = 0; i<len; i++) {
			if(i != 0) {
				String[] neighbors = Arrays.copyOfRange(allKeys, i+1, len);
				CustomMapWritable neighborHash = new CustomMapWritable();
				for(String entryNeighbor : neighbors) {
					if(!entryNeighbor.equals(allKeys[i])) {
						Text currentNeighbor = new Text(entryNeighbor);
						if(!neighborHash.containsKey(entryNeighbor)) neighborHash.put(currentNeighbor, new IntWritable(1));
						else {
							IntWritable currentValue = (IntWritable)neighborHash.get(currentNeighbor);
							neighborHash.put(currentNeighbor, new IntWritable(currentValue.get() + 1));
						}
					} else {
						break;
					}
				}
				if(!recordHash.containsKey(allKeys[i]))	recordHash.put(allKeys[i], neighborHash);
				else {
					for(Writable entryKey : neighborHash.keySet()) {
						if(recordHash.get(allKeys[i]).containsKey(entryKey)) {
							IntWritable hashEntryValue = (IntWritable)recordHash.get(allKeys[i]).get(entryKey);
							IntWritable neighborEntryValue = (IntWritable)neighborHash.get(entryKey);
							recordHash.get(allKeys[i]).put(entryKey, new IntWritable(hashEntryValue.get() + neighborEntryValue.get()));
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
			for(Writable keys : recordHash.get(entry).keySet()) {
				System.out.println("here " + entry);
				System.out.println(keys + " " + recordHash.get(entry).get(keys));
			}
			context.write(new Text(entry), recordHash.get(entry));
		}
		recordHash.clear();
	}
}
