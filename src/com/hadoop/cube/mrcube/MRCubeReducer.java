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

public class MRCubeReducer extends Reducer<Segment,
											LongWritable, 
											Tuple, 
											LongWritable> { 
	
	List<BUC> bucs = new ArrayList<BUC>();
	int prevBucId = -1;
	BUC currBUC = null;
	LongWritable long_writable = new LongWritable(0);
	int count = 0;
	
	public MRCubeReducer() {
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		//System.out.println(count);
		currBUC.finish();
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		//System.out.println("REDUCER");
		
		Configuration conf = context.getConfiguration();
        
	    String[] bucsStr = conf.get("bucsStr").split("z");
	    for(String s: bucsStr){
	    	 bucs.add(new BUC(s, context));
	    }
		
//	    for(Cuboid cuboid : bucs.get(0).cuboids){
//	    	System.out.println(Utils.joinI(cuboid.numPresentation, ""));
//	    }
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Segment segment, Iterable<LongWritable> value, Context context)
		throws IOException, InterruptedException {
		
		if (prevBucId != segment.id){
			if (prevBucId != -1){
				currBUC.finish();
			}
			
			currBUC = bucs.get(segment.id);
			
			prevBucId = segment.id;
			//System.out.println("Batch "+ prevBucId);
		}	
		
		long sum = 0;
		for (LongWritable lw : value) {
			sum += lw.get();
		}
				
		//System.out.println(segment + "\t" + sum);

		currBUC.addTuple(new Tuple(segment.tuple.fields), sum);
	}
}
