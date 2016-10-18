package com.suman.BD.StripesMethod;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class CustomMapWritable extends MapWritable {
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder(); 
		for(Writable entryKey : this.keySet()){
			sb.append(entryKey);
			sb.append(" ");
			sb.append(this.get(entryKey));
			sb.append(" ");
		}
		return sb.toString();
	}
}
