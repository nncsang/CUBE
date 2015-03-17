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

public class MRCubeMapper extends Mapper<LongWritable, Text, Segment, LongWritable> { 
	
	
	Segment segment;
	public int nBatch;
	public int[] nullArray;
	public List<Batch> unfriendlyBatches;
	public Tuple tuple;
	
	private LongWritable sum = new LongWritable(0);
	private int[] indexMap = {2,6,8,10,19,20};
	
	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Segment, LongWritable>.Context context) throws IOException, InterruptedException {
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
	protected void map(LongWritable idx, Text line, Context context) throws IOException, InterruptedException {
    	
    	String[] values = line.toString().split(" ");
		sum.set(Integer.parseInt(values[21]));
		
		Tuple key = new Tuple();
		key.fields[0] = Integer.parseInt(values[2]);
		key.fields[1] = Integer.parseInt(values[6]);
		key.fields[2] = Integer.parseInt(values[8]);
		key.fields[3] = Integer.parseInt(values[10]);
		key.fields[4] = Integer.parseInt(values[19]);
		key.fields[5] = Integer.parseInt(values[20]);
		
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
    		context.write(segment, sum);
    	}
    	
    	//System.out.println(key);
    	for (int i = 0; i < nBatch; i++){
    		segment = new Segment(i, key.fields);
    		context.write(segment, sum);
    		//System.out.println(segment);
    	}
    	
    	//System.out.println("---------------------------------");
    }
}
