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

import com.hadoop.cube.buc.BUC;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class MRCubeEstimateReducer extends Reducer<Segment,
											LongWritable, 
											Tuple, 
											LongWritable> { 
	
	public static LongWritable one = new LongWritable(1);
	public static LongWritable zero = new LongWritable(0);
	
	public MRCubeEstimateReducer() {
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Segment segment, Iterable<LongWritable> value, Context context)
		throws IOException, InterruptedException {
		//System.out.println(segment);
		context.write(segment.tuple, zero);
	}
}
