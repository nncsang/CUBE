package com.hadoop.cube.irg_plus_irg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.hadoop.cube.data_writable.Segment;

public class IRGPlusIRGCombiner extends Reducer<Segment,
									LongWritable,
									Segment,
									LongWritable> {

	public IRGPlusIRGCombiner() {
	
	}
	
	@Override
	protected void reduce(Segment key, Iterable<LongWritable> value, Context context)
	    throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable lw : value) {
		    sum += lw.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
