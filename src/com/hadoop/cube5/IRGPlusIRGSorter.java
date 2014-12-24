package com.hadoop.cube5;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.cube2rollups.GlobalSettings;

public class IRGPlusIRGSorter extends WritableComparator{

	protected IRGPlusIRGSorter() {
		super(Text.class, true);
	} 
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {	
		TupleWritable5 key1 = ((TupleWritable5) a);
		TupleWritable5 key2 = ((TupleWritable5) b);
		
		return key1.compareTo(key2);
	}

}
