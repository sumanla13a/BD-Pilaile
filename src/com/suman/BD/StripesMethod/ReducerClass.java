package com.suman.BD.StripesMethod;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * @author: Suman Lama
 * @namespace ReducerClass
 * @memberOf com.suman.BD.StripesMethod
 * @description: 
 * Reducer class
 * */
public class ReducerClass extends Reducer<Text, CustomMapWritable, Text, CustomMapWritable> {
	/*
	 * @memberOf com.suman.BD.StripesMethod.ReducerClass
	 * @method reduce, main reducer method. This is called recursively with each input mapper output sorted and grouped by framework
	 * 
	 * @params {Text} key, the main element for which other occurence is calculated
	 * @params {Iterable<CustomMapWritable>} values, The values of all elements in a stripe. @ref com.suman.BD.StripesMethod.CustomMapWritable
	 * @params {Context} context
	 * @throws IOException, InterruptedException
	 * 
	 * @description: 
	 * Calculates occurence frequency of each item after a certain item (say A) till the same item (A) re-occurs
	 * Uses Stripes method i.e each relation is stored in an associative array and output is in same type
	 * */
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
					DoubleWritable entryVal = hashMapConverted.get(entryKey);
					newMap.put(entryKey, entryVal);
				} else {
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
