package com.suman.BD.HybridMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.suman.BD.Pair.*;

public class MapperClass extends Mapper<LongWritable, Text, Pair, DoubleWritable> {
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
							if(!recordHash.containsKey(currentPair)) {
								recordHash.put(currentPair, 1);
							}
							else {
								recordHash.put(currentPair, (int)recordHash.get(currentPair) + 1);
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
				context.write(i, new DoubleWritable((int)recordHash.get(i)));
			}
			recordHash.clear();
		}
}
