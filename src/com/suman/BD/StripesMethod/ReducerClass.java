package com.suman.BD.StripesMethod;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, CustomMapWritable, Text, CustomMapWritable> {
	@Override
	public void reduce(Text key, Iterable<CustomMapWritable> values, Context context) throws IOException, InterruptedException {
		HashMap<String, DoubleWritable> newMap = new HashMap<String, DoubleWritable>();
		double marginal = 0.0;
		for(CustomMapWritable mw : values) {
			HashMap<String, DoubleWritable> hashMapConverted = new HashMap<String, DoubleWritable>();
			for(Writable eachEntry : mw.keySet()) {
				hashMapConverted.put(eachEntry.toString(), (DoubleWritable)mw.get(eachEntry));
			}
			for(String entryKey : hashMapConverted.keySet()) {
				DoubleWritable currentValue = hashMapConverted.get(entryKey);
				marginal += currentValue.get();
				if(!newMap.containsKey(entryKey)) {
					System.out.println("h");
					DoubleWritable entryVal = hashMapConverted.get(entryKey);
					newMap.put(entryKey, entryVal);
				} else {
					System.out.println("b");
					DoubleWritable entryVal = hashMapConverted.get(entryKey);
					DoubleWritable prevVal = newMap.get(entryKey);
					newMap.put(entryKey, new DoubleWritable(entryVal.get() + prevVal.get()));
				}
			}
		}
		for(String entryKey : newMap.keySet()) {
			DoubleWritable finalValue = newMap.get(entryKey);
			DoubleWritable frequency = new DoubleWritable(finalValue.get()/marginal);
			newMap.put(entryKey, frequency);
		}
		CustomMapWritable mapWritable = new CustomMapWritable();
		for(String eachEntry : newMap.keySet()) {
			mapWritable.put(new Text(eachEntry), newMap.get(eachEntry));
		}
		context.write(key, mapWritable);
		newMap.clear();
		mapWritable.clear();
	}
}
