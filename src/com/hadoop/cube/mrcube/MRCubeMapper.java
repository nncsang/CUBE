package com.hadoop.cube.mrcube;

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

import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.data_writable.Tuple;

public class MRCubeMapper extends Mapper<Tuple, LongWritable, Segment, LongWritable> { 
	
	List<Batch> friendlyBatches;
	List<Batch> unfriendlyBatches;

	@Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<Tuple, LongWritable, Segment, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        
        String[] friendlyBatchStr = conf.get("friendlyBatches").split("=");
        String[] unfriendlyBatchStr = conf.get("unfriendlyBatches").split("=");
        
        friendlyBatches = new ArrayList<Batch>();
        unfriendlyBatches = new ArrayList<Batch>();
        
        for(String batch: friendlyBatchStr){
        	friendlyBatches.add(new Batch(batch));
        }
        
        for(String batch: unfriendlyBatchStr){
        	unfriendlyBatches.add(new Batch(batch));
        }
        
        for(Batch batch: friendlyBatches){
        	batch.print();
        }
        
        for(Batch batch: unfriendlyBatches){
        	batch.print();
        }
    }

    @Override
	protected void map(Tuple key, 
	        LongWritable value,
			Context context) throws IOException, InterruptedException {
    	
    }
}
