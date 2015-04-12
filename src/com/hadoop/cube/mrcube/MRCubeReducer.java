package com.hadoop.cube.mrcube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.hadoop.cube.buc.BUC;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class MRCubeReducer extends Reducer<Segment,
											LongWritable, 
											Tuple, 
											LongWritable> { 
	
	List<BUC> bucs = new ArrayList<BUC>();
	int prevBucId = -1;
	int prevId = 0;
	Tuple prevTuple;
	int[] nullArray;
	
	long SUM = 0;
	BUC currBUC = null;
	LongWritable long_writable = new LongWritable(0);
	int count = 0;
	
	private MultipleOutputs out;
	private MultipleOutputs out1;
	public MRCubeReducer() {
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		//System.out.println(count);
		currBUC.finish();
		if (prevId != 0){
			long_writable.set(SUM);
			out1.write("friendly", prevTuple, long_writable);
		}
		out.close();
		out1.close();
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		
		Tuple.setLength(Integer.parseInt(conf.get("length")));
		
		out = new MultipleOutputs<Tuple, LongWritable>(context);
		out1 = new MultipleOutputs<Tuple, LongWritable>(context);
	    String[] bucsStr = conf.get("bucsStr").split("z");
	    for(String s: bucsStr){
	    	 bucs.add(new BUC(s, out));
	    }
	    
	    nullArray = new int[Tuple.length];
        Arrays.fill(nullArray, -1);
        
        prevTuple = new Tuple(nullArray);
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Segment segment, Iterable<LongWritable> value, Context context)
		throws IOException, InterruptedException {
		
		long sum = 0;
		for (LongWritable lw : value) {
			sum += lw.get();
		}
		
		Tuple tuple = new Tuple(segment.tuple.fields);
		if (segment.id >= 0){
			if (prevBucId != segment.id){
				if (prevBucId != -1){
					currBUC.finish();
				}
				
				currBUC = bucs.get(segment.id);
				
				prevBucId = segment.id;
				//System.out.println("Batch "+ prevBucId);
			}			
			//System.out.println(segment + "\t" + sum);
	
			currBUC.addTuple(tuple, sum);
		}else{
			//System.out.println(segment + "\t" + sum);
			if (prevId != segment.id){
				if (prevId != 0){
					long_writable.set(SUM);
					out1.write("friendly", prevTuple, long_writable);	
				}
				
				SUM = 0;
				prevId = segment.id;
			}else{
				if (prevTuple.compareTo(tuple) != 0){
					long_writable.set(SUM);
					out1.write("friendly", prevTuple, long_writable);
					SUM = 0;
				}
			}
			
			prevTuple = tuple;
			SUM += sum; 
		}
	}
}
