package com.hadoop.cube.mrcube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.data_writable.Tuple;

public class MRCubeMapper extends Mapper<Tuple, LongWritable, Segment, LongWritable> { 
	
	
	Segment segment;
	public int nBatch;
	public int[] nullArray;
	public List<Batch> unfriendlyBatches;
	public Tuple tuple;
	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<Tuple, LongWritable, Segment, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        //System.out.println("MAPPER:");
        Configuration conf = context.getConfiguration();
        
        nBatch = Integer.parseInt(conf.get("nBatch"));
        String[] strs = conf.get("unfriendlyBatches").split("=");
        
        unfriendlyBatches = new ArrayList<Batch>();
        for(String str: strs){
        	Batch batch = new Batch(str);
        	unfriendlyBatches.add(batch);
        	Segment.partitionFactor.add(batch.partition_factor);
        }
        
        nullArray = new int[Tuple.length];
        Arrays.fill(nullArray, -1);
    }

    @Override
	protected void map(Tuple key, LongWritable value, Context context) throws IOException, InterruptedException {
    	
    	for(int i = 0; i < unfriendlyBatches.size(); i++){
    		Cuboid cuboid = unfriendlyBatches.get(i).cuboids.get(0);
    		
    		segment = new Segment(-i - 1, nullArray);
    		if (cuboid.numPresentation != null){
    			for(int j = 0; j < cuboid.numPresentation.size(); j++){
    				int index = cuboid.numPresentation.get(j);
    				segment.tuple.fields[index] = key.fields[index];
    			}
    		}
    		
    		//System.out.println("MAPPER: \t" + segment + "\t" + value);
    		context.write(segment, value);
    	}
    	
    	//System.out.println(key);
    	for (int i = 0; i < nBatch; i++){
    		segment = new Segment(i, key.fields);
    		context.write(segment, value);
    		//System.out.println(segment);
    	}
    	
    	//System.out.println("---------------------------------");
    }
}
