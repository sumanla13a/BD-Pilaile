package com.suman.BD.HybridMethod;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.suman.BD.Pair.Pair;
import com.suman.BD.StripesMethod.CustomMapWritable;

public class ReducerClass extends Reducer<Pair, DoubleWritable, Text, CustomMapWritable> {
	private int marginal;
	private HashMap<String, DoubleWritable> recordHash;
	private String currentTerm;
	@Override
	public void setup(Context context) {
		marginal = 0;
		recordHash = new HashMap<String, DoubleWritable>();
		currentTerm = null;
	}
	@Override
	public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		/*System.out.println("pair " + pair.toString());
		for(DoubleWritable d : values) {
			System.out.println(d.get());
		}*/
		if(currentTerm == null) currentTerm = pair.getKey();
		
		else if(!currentTerm.equals(pair.getKey())) {
			/*System.out.println("Marginal for " + currentTerm + " = " + marginal + " with entry value = ");
			for(String entryKey : recordHash.keySet()) {
				System.out.println(recordHash.get(entryKey));
			}*/
			System.out.println("done");
			for(String entryKey : recordHash.keySet()) {
				DoubleWritable entryValue = recordHash.get(entryKey);
				System.out.println("Getting value of key " + currentTerm + " for " + entryKey + " comes " + entryValue.get());
				recordHash.put(entryKey, new DoubleWritable(entryValue.get()/marginal));
				entryValue = null;
			}
			
			CustomMapWritable mapWritable = new CustomMapWritable();
			for(String eachEntry : recordHash.keySet()) {
				mapWritable.put(new Text(eachEntry), recordHash.get(eachEntry));
			}
			
			context.write(new Text(currentTerm), mapWritable);
			recordHash.clear();
			marginal = 0;
			currentTerm = pair.getKey();
		}
		String currentValuePair = pair.getValue();
		for(DoubleWritable value : values) {
			marginal += value.get();
			if(!recordHash.containsKey(currentValuePair)) {
				recordHash.put(currentValuePair, new DoubleWritable(value.get()));
				System.out.println("For main element " + pair.getKey() + " setting value for " + currentValuePair + " to " + value.get());
			} else {
				DoubleWritable currentVal = recordHash.get(currentValuePair);
				recordHash.put(currentValuePair, new DoubleWritable(currentVal.get() + value.get()));
			}
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException  {
		for(String entryKey : recordHash.keySet()) {
			DoubleWritable entryValue = recordHash.get(entryKey);
			recordHash.put(entryKey, new DoubleWritable((double)entryValue.get()/(double)marginal));
		}
		CustomMapWritable mapWritable = new CustomMapWritable();
		for(String eachEntry : recordHash.keySet()) {
			mapWritable.put(new Text(eachEntry), recordHash.get(eachEntry));
		}
		context.write(new Text(currentTerm), mapWritable);
	}
}
