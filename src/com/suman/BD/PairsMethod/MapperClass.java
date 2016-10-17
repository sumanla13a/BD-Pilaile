package com.suman.BD.PairsMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.suman.BD.Pair.*;

public class MapperClass extends Mapper<LongWritable, Text, Pair, IntWritable> {
		private HashMap<Pair, Object> recordHash;
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			recordHash = new HashMap<Pair, Object>();
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] allKeys = line.split(" ");
			int len = allKeys.length;
			for(int i = 0; i<len; i++) {
				if(i != 0) {
					String[] neighbors = Arrays.copyOfRange(allKeys, i+1, len);
					for(String eachNeighbor : neighbors) {
						if(!eachNeighbor.equals(allKeys[i])) {
							Pair currentPair = new Pair(allKeys[i], eachNeighbor);
							Pair starPair = new Pair(allKeys[i],  "*");
							if(!recordHash.containsKey(currentPair)) {
								recordHash.put(currentPair, 1);
							}
							else {
								recordHash.put(currentPair, (int)recordHash.get(currentPair) + 1);
							}

							if(!recordHash.containsKey(starPair)) {
								recordHash.put(starPair, 1);
							} else {
								recordHash.put(starPair, (int)recordHash.get(starPair) + 1);
							}
					
						} else {
							break;
						}
					}
				}
			}
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(Pair i : recordHash.keySet()) {
				System.out.println(i.toString());
				System.out.println((int)recordHash.get(i));
				context.write(i, new IntWritable((int)recordHash.get(i)));
			}
			recordHash.clear();
		}
}
