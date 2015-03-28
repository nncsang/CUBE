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

public class MRCubeEstimateReducer extends Reducer<Segment,
											LongWritable, 
											LongWritable, 
											Text> { 
	
	public static LongWritable one = new LongWritable(1);
	public static LongWritable zero = new LongWritable(0);
	public int prevCuboidId = -1;
	public Tuple preTuple = null;
	public long currMax = -1;
	public long currSum = 0;
	
	public MRCubeEstimateReducer() {
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		if (currSum > currMax){
			currMax = currSum;
			currSum = 0;
		}
		context.write(new LongWritable(prevCuboidId), new Text(Long.toString(currMax)));
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
//		if (segment.id == 64)
//			segment.id = 64;
		long sum = 0;
		for (LongWritable lw : value) {
			sum += lw.get();
		}
		
		Tuple tuple = new Tuple(segment.tuple.fields);
		if (segment.id == prevCuboidId){
			if (tuple.compareTo(preTuple) != 0){
				if (currSum > currMax){
					currMax = currSum;
					currSum = 0;
				}
			}
		}else{
			if (prevCuboidId != -1){
				if (currSum > currMax){
					currMax = currSum;
					currSum = 0;
				}
				context.write(new LongWritable(prevCuboidId), new Text(Long.toString(currMax)));
			}
			currMax = -1;
			prevCuboidId = segment.id;
		}
		
		preTuple = tuple;
		currSum += sum;
	}
}
