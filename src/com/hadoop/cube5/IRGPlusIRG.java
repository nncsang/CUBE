package com.hadoop.cube5;


import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.cube.TimeStampWritable;
import com.hadoop.cube5.Checker;
import com.hadoop.cube2rollups.Cube;
import com.hadoop.cube2rollups.GlobalSettings;
import com.hadoop.cube2rollups.HeuristicBasedConverter;
import com.hadoop.cube2rollups.RollUp;
import com.hadoop.cube2rollups.Utils;


public class IRGPlusIRG extends Configured implements Tool{
	private int numReducers;
	private Path inputPath;
	private Path outputDir;
	private int pivot;
	
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IRGPlusIRG(args), args);
		System.exit(res);
	}
	
	public IRGPlusIRG(String[] args) {
		if (args.length != 4) {
			System.out
					.println("Usage: IRGPlusIRG <input_path> <output_path> <pivot> <num_reducers>");
			System.exit(0);
		}
		this.inputPath = new Path(args[0]);
		this.outputDir = new Path(args[1]);
		this.pivot = Integer.parseInt(args[2]);
		this.numReducers = Integer.parseInt(args[3]);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		Job job = new Job(conf, "IRG-Plus-IRG" + this.pivot); 
		
		// set job input format
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set map class and the map output key and value classes
		job.setMapperClass(IRGPlusIRGMapper.class);
		job.setMapOutputKeyClass(TupleWritable5.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(IRGPlusIRGPartitioner.class);
		//job.setSortComparatorClass(IRGPlusIRGSorter.class);
		
		// set reduce class and the reduce output key and value classes
		job.setReducerClass(IRGPlusIRGReducer.class);
		
		//job.setSortComparatorClass(TimestampWritable.Comparator.class);

		// set job output format
		job.setOutputKeyClass(TimeStampWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setCombinerClass(IRGPlusIRGCombiner.class);
		
		// add the input file as job input (from HDFS) to the variable
		// inputFile
		FileInputFormat.addInputPath(job, inputPath);

		// set the output path for the job results (to HDFS) to the
		// variable
		// outputPath
		//if file output is existed, delete it
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
				
		FileOutputFormat.setOutputPath(job, outputDir);

		// set the number of reducers using variable numberReducers
		job.setNumReduceTasks(this.numReducers);

		// set the jar class
		job.setJarByClass(IRGPlusIRG.class);
		
		Random tmp = new Random();
        int choosen = (tmp.nextInt() & Integer.MAX_VALUE);
		
		String[] attributes = {"1", "2", "3", "4", "5"};
		Cube cube = new Cube(attributes);
		List<RollUp> rollups = cube.toRollUps(new HeuristicBasedConverter(), pivot);
		
		String rollupList = "";
		String regionList = "";
		String tupleList = "";
		
		int rollupSize = rollups.size();
		
		rollupList = rollupList + rollups.get(0);
		regionList = regionList + Utils.joinI(rollups.get(0).enabledRegions, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
		tupleList = tupleList + Utils.joinB(rollups.get(0).isNeedEmitTuple, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
		
		for(int i = 1; i < rollupSize; i++){
			rollupList = rollupList + GlobalSettings.DELIM_BETWEEN_ROLLUPS + rollups.get(i);
			regionList = regionList + GlobalSettings.DELIM_BETWEEN_ROLLUPS + Utils.joinI(rollups.get(i).enabledRegions, GlobalSettings.DELIM_BETWEEN_GROUPIDS);
			tupleList = tupleList + GlobalSettings.DELIM_BETWEEN_ROLLUPS + Utils.joinB(rollups.get(i).isNeedEmitTuple, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
		}
		
		//System.out.println(rollupList);
		
		/**
		 * Set configs
		 */
		job.getConfiguration().set("hybrid.pivot", String.valueOf(pivot));
		job.getConfiguration().set("choosen", String.valueOf(choosen));
		job.getConfiguration().set("attributes", Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES));
		job.getConfiguration().set("rollupList", rollupList);
		job.getConfiguration().set("regionList", regionList);
		job.getConfiguration().set("tupleList", tupleList);
		
		job.waitForCompletion(true);
		Checker.main(null);
		return 0;
	}
}
