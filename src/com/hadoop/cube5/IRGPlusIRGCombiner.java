package com.hadoop.cube5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IRGPlusIRGCombiner extends Reducer<TupleWritable5,
									LongWritable,
									TupleWritable5,
									LongWritable> {

	public IRGPlusIRGCombiner() {
	
	}
	
	@Override
	protected void reduce(TupleWritable5 key, Iterable<LongWritable> value, Context context)
	    throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable lw : value) {
		    sum += lw.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
