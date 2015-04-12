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
	private int[] data = new int[Tuple.length];
	
	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Segment, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        //System.out.println("MAPPER:");
        Configuration conf = context.getConfiguration();
        
		Tuple.setLength(Integer.parseInt(conf.get("length")));
		
        nBatch = Integer.parseInt(conf.get("nBatch"));
        String[] strs = conf.get("unfriendlyBatches").split("=");
        
        unfriendlyBatches = new ArrayList<Batch>();
        for(String str: strs){
        	Batch batch = new Batch(str);
        	unfriendlyBatches.add(batch);
        	Segment.partitionFactor.add(batch.partition_factor);
        }
        
        
        String partitionOrderStr[] = conf.get("partitionOrderStr").split(",");
        
        List<List<Integer>> partitionOrder = new ArrayList<List<Integer>>();
        
        for(String str: partitionOrderStr){
        	String ins[] = str.split("-");
        	List<Integer> list = new ArrayList<Integer>();
        	
        	for(String intStr: ins){
        		list.add(Integer.parseInt(intStr));
        	}
        	partitionOrder.add(list);
        }
        
        
        Segment.partitionOrder = partitionOrder;
		Segment.updateSortOrder();
        
        nullArray = new int[Tuple.length];
        Arrays.fill(nullArray, -1);
    }

    @Override
    protected void map(LongWritable idx, Text line, Context context)
			throws IOException, InterruptedException {
		
		String[] values = line.toString().split("\t");
		for(int i = 0; i < Tuple.length; i++){
			data[i] = Integer.parseInt(values[i]);
		}
		
		sum.set(Integer.parseInt(values[Tuple.length]));
    	
    	for(int i = 0; i < unfriendlyBatches.size(); i++){
    		Cuboid cuboid = unfriendlyBatches.get(i).cuboids.get(0);
    		
    		segment = new Segment(-i - 1, nullArray);
    		if (cuboid.numPresentation != null){
    			for(int j = 0; j < cuboid.numPresentation.size(); j++){
    				int index = cuboid.numPresentation.get(j);
    				segment.tuple.fields[index] = data[index];
    			}
    		}
    		
    		//System.out.println("MAPPER: \t" + segment + "\t" + value);
    		context.write(segment, sum);
    	}
    	
    	//System.out.println(key);
    	for (int i = 0; i < nBatch; i++){
    		segment = new Segment(i, data);
    		context.write(segment, sum);
    		//System.out.println(segment);
    	}
    	
    	//System.out.println("---------------------------------");
    }
}
