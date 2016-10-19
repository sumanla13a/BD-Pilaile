package com.suman.BD.HybridMethod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.suman.BD.Pair.Pair;
import com.suman.BD.StripesMethod.CustomMapWritable;

public class ReducerClass extends Reducer<Pair, IntWritable, Text, CustomMapWritable> {
	private int marginal;
	private CustomMapWritable recordHash;
	private String currentTerm;
	@Override
	public void setup(Context context) {
		marginal = 0;
		recordHash = new CustomMapWritable();
		currentTerm = null;
	}
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
				System.out.println("h");
				recordHash.put(currVal, new IntWritable(value.get()));
			} else {
				System.out.println("b");
				IntWritable currentVal = (IntWritable)recordHash.get(pair.getValue());
				recordHash.put(currVal, new IntWritable(currentVal.get() + value.get()));
			}
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException  {
		for(Writable entryKey : recordHash.keySet()) {
			IntWritable entryValue = (IntWritable)recordHash.get(entryKey);
			recordHash.put(entryKey, new DoubleWritable((double)entryValue.get()/(double)marginal));
		}
		context.write(new Text(currentTerm), recordHash);
	}
}
