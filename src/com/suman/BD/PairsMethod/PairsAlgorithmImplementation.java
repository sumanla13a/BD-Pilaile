package com.suman.BD.PairsMethod;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.suman.BD.Pair.*;
/*
 * @author: Suman Lama
 * @namespace PairsAlgorithmImplementation
 * @memberOf com.suman.BD.PairsMethod
 * @description: 
 * Configures the task with i/p, o/p value types and mapper/reducer classes
 * for pairs approach of frequency calculation
 * */
public class PairsAlgorithmImplementation {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PairsAlgorithmImplementation");
		
		job.setJarByClass(PairsAlgorithmImplementation.class);
		
		FileInputFormat.addInputPath(job, new Path("pairsAlgorithmInput")); // Setting input file location. Change
		FileOutputFormat.setOutputPath(job, new Path("pairsAlgorithmOutput")); // Setting output file location. Change
		job.setNumReduceTasks(2);// initializing two reducers
		job.setPartitionerClass(PairsPartitioner.class); // setting custom partitioner
		job.setMapperClass(MapperClass.class); // setting mapper class
		job.setReducerClass(ReducerClass.class); // setting reducer class
		job.setMapOutputKeyClass(Pair.class); // setting output key type of mapper
		job.setMapOutputValueClass(IntWritable.class); // setting output value type of mapper
		job.setOutputKeyClass(Pair.class); // setting final output key type
		job.setOutputValueClass(DoubleWritable.class); // setting final output value type
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
