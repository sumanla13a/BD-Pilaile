package com.suman.BD.StripesMethod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, MapWritable, Text, CustomMapWritable> {
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		CustomMapWritable newMap = new CustomMapWritable();
		int marginal = 0;
		for(MapWritable mw : values) {
			for(Writable entryKey : mw.keySet()) {
				IntWritable currentValue = (IntWritable)mw.get(entryKey);
				marginal += currentValue.get();
				if(!newMap.containsKey(entryKey)) {
					IntWritable entryVal = (IntWritable)mw.get(entryKey);
					newMap.put(entryKey, entryVal);
				}
				else {
					IntWritable entryVal = (IntWritable)mw.get(entryKey);
					IntWritable prevVal = (IntWritable)newMap.get(entryKey);
					newMap.put(entryKey, new IntWritable(entryVal.get() + prevVal.get()));
				}
			}
		}
		for(Writable entryKey : newMap.keySet()) {
			IntWritable finalValue = (IntWritable)newMap.get(entryKey);
			DoubleWritable frequency = new DoubleWritable((double)finalValue.get()/(double)marginal);
			newMap.put(entryKey, frequency);
		}
		context.write(key, newMap);
		newMap.clear();
	}
}
