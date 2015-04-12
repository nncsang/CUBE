package com.hadoop.cube.mrcube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.cube.buc.BUC;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class MRCubeAggregateReducer extends Reducer<Tuple,
											LongWritable, 
											Tuple, 
											LongWritable> { 
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		Tuple.setLength(Integer.parseInt(conf.get("length")));
		super.setup(context);
	}
	
	LongWritable long_writable = new LongWritable(0);
	@Override
	protected void reduce(Tuple key, Iterable<LongWritable> value, Context context)
		throws IOException, InterruptedException {
		
		//System.out.println(segment);
		
		long sum = 0;
		for (LongWritable lw : value) {
			sum += lw.get();
		}
		
		long_writable.set(sum);
		context.write(key, long_writable);
		
	}
}
