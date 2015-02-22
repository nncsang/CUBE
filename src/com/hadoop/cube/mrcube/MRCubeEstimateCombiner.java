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

	public MRCubeEstimateCombiner() {
	}
	
	@Override
	protected void reduce(Segment key, Iterable<LongWritable> value, Context context)
	    throws IOException, InterruptedException {
	}
}
