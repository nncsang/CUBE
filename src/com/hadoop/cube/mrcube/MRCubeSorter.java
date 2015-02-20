package com.hadoop.cube.mrcube;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.cube.old_data_writable.TupleWritable;
import com.hadoop.cube.settings.GlobalSettings;

public class MRCubeSorter extends WritableComparator{

	protected MRCubeSorter() {
		super(Text.class, true);
	} 
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {	
		TupleWritable key1 = ((TupleWritable) a);
		TupleWritable key2 = ((TupleWritable) b);
		
		return key1.compareTo(key2);
	}

}
