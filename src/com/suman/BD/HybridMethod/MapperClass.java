package com.suman.BD.HybridMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.suman.BD.Pair.*;
/*
 * @author: Suman Lama
 * @namespace MapperClass
 * @memberOf com.suman.BD.HybridMethod
 * @description: 
 * Mapper class
 * */
public class MapperClass extends Mapper<LongWritable, Text, Pair, IntWritable> {
		private HashMap<Pair, Object> recordHash;
		/*
		 * @memberOf com.suman.BD.HybridMethod.MapperClass
		 * @description: 
		 * @method setup, initial setup of the mapper. Creates hashmap
		 * @params {Context} context
		 * */
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			recordHash = new HashMap<Pair, Object>();
		}
		/*
		 * @memberOf com.suman.BD.HybridMethod.MapperClass
		 * @method map, main mapper method. This is called recursively with each input lines from files
		 * 
		 * @params {LongWritable} key, the bytecode for the current line
		 * @params {Text} value, the current line
		 * @params {Context} context
		 * @throws IOException, InterruptedException
		 * 
		 * @description: 
		 * Counts occurence of each item after a certain item (say A) till the same item (A) re-occurs
		 * Uses Pairs method i.e each relation is stored in a pair
		 * */
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
		/*
		 * @memberOf com.suman.BD.HybridMethod.MapperClass 
		 * @method cleanup, end process of the mapping
		 * @params {Context} context
		 * @description
		 * Writes the output of mapper
		 * */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(Pair i : recordHash.keySet()) {
				context.write(i, new IntWritable((int)recordHash.get(i)));
			}
			recordHash.clear();
		}
}
