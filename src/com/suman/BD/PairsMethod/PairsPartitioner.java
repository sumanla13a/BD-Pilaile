package com.suman.BD.PairsMethod;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.suman.BD.Pair.Pair;
/*
 * @author: Suman Lama
 * @namespace PairsPartitioner
 * @memberOf com.suman.BD.PairsMethod
 * @description: 
 * Partitions the map output to two reducers according to item number
 * 
 * @Overrides default getPartition method of hashPartition
 * */
public class PairsPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair arg0, IntWritable arg1, int arg2) {
		if(arg2==0) return 0;
		if(Integer.parseInt(arg0.getKey())>50) return 1 % arg2;
		else return 0;
	}

}
