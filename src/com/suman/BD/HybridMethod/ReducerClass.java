package com.suman.BD.HybridMethod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.suman.BD.Pair.Pair;
import com.suman.BD.StripesMethod.CustomMapWritable;
/*
 * @author: Suman Lama
 * @namespace ReducerClass
 * @memberOf com.suman.BD.HybridMethod
 * @description: 
 * Reducer class
 * */
public class ReducerClass extends Reducer<Pair, IntWritable, Text, CustomMapWritable> {
	private int marginal;
	private CustomMapWritable recordHash;
	private String currentTerm;
	/*
	 * @memberOf com.suman.BD.HybridMethod.ReducerClass
	 * @description: 
	 * @method setup, initial setup of the reducer. Creates hashmap, a total sum variable and current term variable
	 * @params {Context} context
	 * */
	@Override
	public void setup(Context context) {
		marginal = 0;
		recordHash = new CustomMapWritable();
		currentTerm = null;
	}
	/*
	 * @memberOf com.suman.BD.HybridMethod.ReducerClass
	 * @method reduce, main reducer method. This is called recursively with each input mapper output sorted and grouped by framework
	 * 
	 * @params {Pair} pair, the pair key and value of main and neighbor element. @ref com.suman.BD.Pair.Pair
	 * @params {Iterable<IntWritable>} values, The values set for all common pair
	 * @params {Context} context
	 * @throws IOException, InterruptedException
	 * 
	 * @description: 
	 * Calculates occurence frequency of each item after a certain item (say A) till the same item (A) re-occurs
	 * Uses Stripes method i.e each relation is stored in an associative array and output is in same type
	 * */
	@Override
	public void reduce(Pair pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		if(currentTerm == null) currentTerm = pair.getKey();
		
		else if(!currentTerm.equals(pair.getKey())) {
			for(Writable entryKey : recordHash.keySet()) {
				IntWritable entryValue = (IntWritable)recordHash.get(entryKey);
				recordHash.put(entryKey, new DoubleWritable((double)entryValue.get()/(double)marginal));
			}
			context.write(new Text(currentTerm), recordHash);
			recordHash.clear();
			marginal = 0;
			currentTerm = pair.getKey();
		}
		Writable currVal = new Text(pair.getValue());
		for(IntWritable value : values) {
			marginal += value.get();
			if(!recordHash.containsKey(currVal)) {
				recordHash.put(currVal, new IntWritable(value.get()));
			} else {
				IntWritable currentVal = (IntWritable)recordHash.get(pair.getValue());
				recordHash.put(currVal, new IntWritable(currentVal.get() + value.get()));
			}
		}
	}
	/*
	 * @memberOf com.suman.BD.HybridMethod.ReducerClass 
	 * @method cleanup, end process of the reducer
	 * @params {Context} context
	 * @description
	 * Writes the output of reducer
	 * */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException  {
		for(Writable entryKey : recordHash.keySet()) {
			IntWritable entryValue = (IntWritable)recordHash.get(entryKey);
			recordHash.put(entryKey, new DoubleWritable((double)entryValue.get()/(double)marginal));
		}
		context.write(new Text(currentTerm), recordHash);
	}
}
