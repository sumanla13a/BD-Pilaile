package com.suman.BD.StripesMethod;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
public class StripesPartitioner extends Partitioner<Text, MapWritable> {

		@Override
		public int getPartition(Text arg0, MapWritable arg1, int arg2) {
			if(arg2==0) return 0;
			if(Integer.parseInt(arg0.toString())>50) return 1 % arg2;
			else return 0;
		}

}
