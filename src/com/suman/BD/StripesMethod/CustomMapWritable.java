package com.suman.BD.StripesMethod;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
/*
 * @author: Suman Lama
 * @namespace CustomMapWritable
 * @memberOf com.suman.BD.StripesMethod
 * @description: 
 * Wrapper class for MapWritable
 * For overriding toString Method
 * Concats all keys and values with space
 * */
public class CustomMapWritable extends MapWritable {
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder(); 
		for(Writable entryKey : this.keySet()){
			sb.append("( ");
			sb.append(entryKey);
			sb.append(" ");
			sb.append(this.get(entryKey));
			sb.append(" ");
			sb.append(" ) ");
		}
		return sb.toString();
	}
}
