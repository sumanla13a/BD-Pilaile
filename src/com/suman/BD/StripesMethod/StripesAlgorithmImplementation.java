package com.suman.BD.StripesMethod;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * @author: Suman Lama
 * @namespace StripesAlgorithmImplementation
 * @memberOf com.suman.BD.StripesMethod
 * @description: 
 * Configures the task with i/p, o/p value types and mapper/reducer classes
 * for stripes approach of frequency calculation
 * */
public class StripesAlgorithmImplementation {


		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "PairsAlgorithmImplementation");
			
			job.setJarByClass(StripesAlgorithmImplementation.class);
			
			FileInputFormat.addInputPath(job, new Path("pairsAlgorithmInput")); // Setting input file location. Change
			FileOutputFormat.setOutputPath(job, new Path("stripesAlgorithmOutput")); // Setting output file location. Change
			job.setNumReduceTasks(2);// initializing two reducers
			job.setPartitionerClass(StripesPartitioner.class);// setting custom partitioner
			job.setMapperClass(MapperClass.class); // setting mapper class
			job.setReducerClass(ReducerClass.class); // setting reducer class
			job.setMapOutputKeyClass(Text.class);// setting output key type of mapper
			job.setMapOutputValueClass(CustomMapWritable.class);// setting output value type of mapper
			job.setOutputKeyClass(Text.class);// setting final output key type
			job.setOutputValueClass(CustomMapWritable.class);// setting final output key type
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
