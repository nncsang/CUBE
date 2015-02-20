package com.hadoop.cube.mrcube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.settings.GlobalSettings;

public class MRCubeReducer extends Reducer<Segment,
											LongWritable, 
											Tuple, 
											LongWritable> { 
	
	public MRCubeReducer() {
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		
	}
	
	@Override
	protected void reduce(Segment segment, Iterable<LongWritable> value, Context context)
		throws IOException, InterruptedException {
	
	}
}
