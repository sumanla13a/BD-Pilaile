package com.suman.BD.PairsMethod;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.suman.BD.Pair.*;

public class PairsAlgorithmImplementation {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PairsAlgorithmImplementation");
		
		job.setJarByClass(PairsAlgorithmImplementation.class);
		
		FileInputFormat.addInputPath(job, new Path("pairsAlgorithmInput")); // Setting input file location. Change
		FileOutputFormat.setOutputPath(job, new Path("pairsAlgorithmOutput")); // Setting output file location. Change
		job.setNumReduceTasks(2);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
