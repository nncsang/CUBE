package com.hadoop.cube.irg_plus_irg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.hadoop.cube.AirlineWritable;
import com.hadoop.cube.TupleWritable;
import com.hadoop.cube.data_structure.Region;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class IRGPlusIRGMapper extends Mapper<AirlineWritable, LongWritable, TupleWritable, LongWritable> { 

	private Map<String, Integer> attrPosition;
	
	private List<RollUp> rollups;
	private int size = 0;
	protected int pivot = -1;
	
	private TupleWritable tuple1;
	private TupleWritable tuple2;

	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<AirlineWritable, LongWritable, TupleWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        
        this.pivot = Integer.parseInt(conf.get("hybrid.pivot", "-1"));
        this.attrPosition = new HashMap<String, Integer>();
        
        String[] attributes = conf.get("attributes").split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
   
        for(int i = 0; i < attributes.length; i++){
        	this.attrPosition.put(attributes[i], i);
        }
        
        String[] rollupListString = conf.get("rollupList").split(GlobalSettings.DELIM_BETWEEN_ROLLUPS);
        String[] tupleListString = conf.get("tupleList").split(GlobalSettings.DELIM_BETWEEN_ROLLUPS);
		
       
		this.rollups = new ArrayList<RollUp>();
		for(int i = 0; i < rollupListString.length; i++){
			RollUp rollup = new RollUp(rollupListString[i].split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES));
			
			String[] tupleArray = tupleListString[i].split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
			rollup.isNeedEmitTuple[0] = Boolean.valueOf(tupleArray[0]);
			rollup.isNeedEmitTuple[1] = Boolean.valueOf(tupleArray[1]);
			
			this.rollups.add(rollup);
		}
		
		this.size = rollups.size();
		
		this.tuple1 = new TupleWritable();
		this.tuple2 = new TupleWritable();
    }

    @Override
	protected void map(AirlineWritable key, 
	        LongWritable value,
			Context context) throws IOException, InterruptedException {
    	int testId = 16;
    	for(int i = 0; i < this.size; i++){
    		
    		this.tuple1.id = i;
    		this.tuple2.id = i;
    		
    		RollUp rollup = rollups.get(i);
    		
    		String[] attributes = rollup.getAttributes();
    		int length = attributes.length;
    		
    		boolean[] isNeedEmitTuple = rollup.isNeedEmitTuple;
    		
    		for(int j = 0; j < length; j++){
    			
    			int attr = key.fields[attrPosition.get(attributes[j])]; 
    		
	    		if (j >= this.pivot){
	    			this.tuple1.airlineWritable.fields[j] = AirlineWritable.NullValue;
	    		}else{
	    			this.tuple1.airlineWritable.fields[j] = attr;
	    		}
    			
    			this.tuple2.airlineWritable.fields[j] = attr;
    		}
    		
    		if (isNeedEmitTuple[0] == true)
    			context.write(this.tuple1, value);
    		
    		if (isNeedEmitTuple[1] == true){
    			context.write(this.tuple2, value);
    		}
    	}
    }
}
