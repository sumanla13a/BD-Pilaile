package com.suman.BD.HybridMethod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.suman.BD.Pair.Pair;
import com.suman.BD.PairsMethod.PairsPartitioner;
import com.suman.BD.StripesMethod.CustomMapWritable;

public class HybridAlgorithmImplementation {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PairsAlgorithmImplementation");
		
		job.setJarByClass(HybridAlgorithmImplementation.class);
		
		FileInputFormat.addInputPath(job, new Path("pairsAlgorithmInput")); // Setting input file location. Change
		FileOutputFormat.setOutputPath(job, new Path("hybridAlgorithmOutput")); // Setting output file location. Change
		job.setNumReduceTasks(2);
		job.setPartitionerClass(PairsPartitioner.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CustomMapWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
