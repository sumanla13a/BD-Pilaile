package com.suman.BD.PairsMethod;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.suman.BD.Pair.*;

public class ReducerClass extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	private int marginal;
	@Override
	public void setup(Context context) {
		marginal = 0;
	}
	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		/*System.out.println("Reducers");
		System.out.println(key.toString());
		System.out.println("vaues");*/
		/*for(IntWritable value : values) {
			System.out.println(value.get());
		}*/
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