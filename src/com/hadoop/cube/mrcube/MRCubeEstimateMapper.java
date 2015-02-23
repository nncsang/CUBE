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

public class MRCubeEstimateMapper extends Mapper<Tuple, LongWritable, Segment, LongWritable> { 
	
	public static LongWritable one = new LongWritable(1);
	private List<Cuboid> regions;
	
	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<Tuple, LongWritable, Segment, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        
		String[] regionListString = conf.get("regionList").split(GlobalSettings.DELIM_BETWEEN_CONTENTS_OF_TUPLE);
		
		this.regions = new ArrayList<Cuboid>();
		
		for(int i = 0; i < regionListString.length; i++){
			regions.add(new Cuboid(regionListString[i].split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES)));
		}
        
    }

    @Override
	protected void map(Tuple value, LongWritable index, Context context) throws IOException, InterruptedException {
    	//System.out.println(value);
    	
    	int random = Utils.randInt(0, 100);
    	if (random > GlobalSettings.RANDOM_RATE)
    		return;
    	
    	int size = regions.size();
		for(int i = 0; i < size; i++){
			Cuboid region = regions.get(i);
			String[] attributes = region.getAttributes();
			int length = attributes.length;
			
			Tuple key = new Tuple();
			for(int j = 0; j < length; j++){
				if (attributes[j].equals(GlobalSettings.ALL)){
					key.fields[j] = Tuple.NullValue;
				}else{
					key.fields[j] = value.fields[j];
				}
			}
			
			context.write(new Segment(i, key.fields), one);
		}
    }
}
