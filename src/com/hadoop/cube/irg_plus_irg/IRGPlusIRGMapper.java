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

import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.data_writable.Tuple;

public class IRGPlusIRGMapper extends Mapper<LongWritable, Text, Segment, LongWritable> { 

	private Map<String, Integer> attrPosition;
	
	private List<RollUp> rollups;
	private int size = 0;
	protected int pivot = -1;
	
	private Segment segment1;
	private Segment segment2;
	
	private int[] data = new int[Tuple.length];
	private LongWritable sum = new LongWritable(0);
	
	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Segment, LongWritable>.Context context) throws IOException, InterruptedException {
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
		
		this.segment1 = new Segment();
		this.segment2 = new Segment();
    }

    @Override
	protected void map(LongWritable index, 
	        Text line,
			Context context) throws IOException, InterruptedException {
    	
    	String[] values = line.toString().split("\t");
    	for(int i = 0; i < Tuple.length; i++){
			data[i] = Integer.parseInt(values[i]);
		}
    	
		sum.set(Integer.parseInt(values[Tuple.length]));

    	
    	for(int i = 0; i < this.size; i++){
    		
    		this.segment1.id = i;
    		this.segment2.id = i;
    		
    		RollUp rollup = rollups.get(i);
    		
    		String[] attributes = rollup.getAttributes();
    		int length = attributes.length;
    		
    		boolean[] isNeedEmitTuple = rollup.isNeedEmitTuple;
    		
    		for(int j = 0; j < length; j++){
    			
    			int attr = data[attrPosition.get(attributes[j])]; 
    		
	    		if (j >= this.pivot){
	    			this.segment1.tuple.fields[j] = Tuple.NullValue;
	    		}else{
	    			this.segment1.tuple.fields[j] = attr;
	    		}
    			
    			this.segment2.tuple.fields[j] = attr;
    		}
    		
    		if (isNeedEmitTuple[0] == true)
    			context.write(this.segment1, sum);
    		
    		if (isNeedEmitTuple[1] == true){
    			context.write(this.segment2, sum);
    		}
    	}
    }
}
