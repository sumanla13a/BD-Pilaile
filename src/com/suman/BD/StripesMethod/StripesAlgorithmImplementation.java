package com.suman.BD.StripesMethod;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesAlgorithmImplementation {


		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "PairsAlgorithmImplementation");
			
			job.setJarByClass(StripesAlgorithmImplementation.class);
			
			FileInputFormat.addInputPath(job, new Path("pairsAlgorithmInput")); // Setting input file location. Change
			FileOutputFormat.setOutputPath(job, new Path("stripesAlgorithmOutput")); // Setting output file location. Change
//			job.setNumReduceTasks(2);
//			job.setPartitionerClass(PairsPartitioner.class);
			job.setMapperClass(MapperClass.class);
			job.setReducerClass(ReducerClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MapWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
