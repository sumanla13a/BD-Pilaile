package com.suman.BD.PairsMethod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.suman.BD.Pair.*;

/*
 * @author: Suman Lama
 * @namespace ReducerClass
 * @memberOf com.suman.BD.PairsMethod
 * @description: 
 * Reducer class
 * */
public class ReducerClass extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	private int marginal;
	/*
	 * @memberOf com.suman.BD.PairsMethod.ReducerClass
	 * @description: 
	 * @method setup, initial setup of the reducer. Creates hashmap, a total sum variable and current term variable
	 * @params {Context} context
	 * */
	@Override
	public void setup(Context context) {
		marginal = 0;
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
	 * Uses Pairs method i.e each relation is stored in a Pair
	 * */
	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		if(key.getValue().equals("*")) {
			marginal = 0;
			for(IntWritable i : values)
				marginal += i.get();
		} else {
			int sum = 0;
			for(IntWritable i : values) 
				sum += i.get();
			DoubleWritable probability = new DoubleWritable((double)sum / (double)marginal);
			context.write(key, probability);
		}
			
	}
}