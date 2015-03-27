package com.hadoop.cube.mrcube;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.hadoop.cube.data_writable.Segment;

public class MRCubeEstimateCombiner extends Reducer<Segment,
									LongWritable,
									Segment,
									LongWritable> {
	
	public LongWritable one = new LongWritable(1); 
	public MRCubeEstimateCombiner() {
	}
	
	@Override
	protected void reduce(Segment key, Iterable<LongWritable> value, Context context)
	    throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable lw : value) {
		    sum += lw.get();
		}
		//one.set(sum);
		
		context.write(key, one);
	}
}
