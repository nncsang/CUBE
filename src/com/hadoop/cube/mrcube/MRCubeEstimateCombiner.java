package com.hadoop.cube.mrcube;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.settings.GlobalSettings;

public class MRCubeEstimateCombiner extends Reducer<Segment,
									LongWritable,
									Segment,
									LongWritable> {
	
	public LongWritable one = new LongWritable(1);
	public int special_ids = -1;
	
	public MRCubeEstimateCombiner() {
	}
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		String[] regionListString = conf.get("regionList").split(GlobalSettings.DELIM_BETWEEN_CONTENTS_OF_TUPLE);
		special_ids = regionListString.length;
		
		super.setup(context);
	}
	
	@Override
	protected void reduce(Segment key, Iterable<LongWritable> value, Context context)
	    throws IOException, InterruptedException {
		
		if (key.id == special_ids){
			long sum = 0;
			for (LongWritable lw : value) {
			    sum += lw.get();
			}
			one.set(sum);
		}else{
			one.set(1);
		}
		
		context.write(key, one);
	}
}
