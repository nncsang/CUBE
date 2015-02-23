package com.hadoop.cube.mrcube;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.cube.data_structure.CubeLattice;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class MRCubeEstimate extends Configured implements Tool{

	private int numReducers;
	private Path inputPath;
	private Path outputDir;
	private int tupleLength;
	
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRCubeEstimate(args), args);
		System.exit(res);
	}
	
	public MRCubeEstimate(String[] args) {
		if (args.length != 4) {
			System.out.println("Usage: MRCubeEstimate <input_path> <output_path> <num_reducers> <tuple_length>");
			System.exit(0);
		}
		
		this.inputPath = new Path(args[0]);
		this.outputDir = new Path(args[1]);
		this.numReducers = Integer.parseInt(args[2]);
		this.tupleLength = Integer.parseInt(args[3]);
		
		Tuple.setLength(tupleLength);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		Job job = new Job(conf, "MRCubeEstimate"); 
		
		// set job input format
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set map class and the map output key and value classes
		job.setMapperClass(MRCubeEstimateMapper.class);
		job.setMapOutputKeyClass(Segment.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(MRCubeEstimatePartitioner.class);
		//job.setSortComparatorClass(IRGPlusIRGSorter.class);
		
		// set reduce class and the reduce output key and value classes
		job.setReducerClass(MRCubeEstimateReducer.class);
		
		//job.setSortComparatorClass(TimestampWritable.Comparator.class);

		// set job output format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setCombinerClass(MRCubeEstimateCombiner.class);
		
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
		job.setJarByClass(MRCubeEstimate.class);
		
		/** SET UP **/
		String[] attributes = new String[this.tupleLength];
		
		for(int i = 0; i < this.tupleLength; i++)
			attributes[i] = Integer.toString(i);
		Tuple.setLength(tupleLength);
		
		CubeLattice cube = new CubeLattice(attributes);
		List<Cuboid> cuboids = cube.cuboids();
		String regionList = "";
		for(int i = 0; i < cuboids.size(); i++){
			String region = cuboids.get(i).toString();
			regionList = regionList + region + GlobalSettings.DELIM_BETWEEN_CONTENTS_OF_TUPLE;
		}
		
		regionList = regionList.substring(0, regionList.length() - 1);
		
		job.getConfiguration().set("attributes", Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES));
		job.getConfiguration().set("regionList", regionList);
		
		/** RUN **/
		job.waitForCompletion(true);
		
		try{
	        FileStatus[] status = fs.listStatus(outputDir);
	 
	        for (int i = 0; i < status.length; i++){
	 
	            BufferedReader brIn=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
	            String line;
	            line=brIn.readLine();
	 
	            while (line != null){
	            	System.out.println(line);
	                line=brIn.readLine();
	            }
	        }
	 
	    }catch(Exception e){
	        System.out.println(e.toString());
	    }
		
		return 0;
	}
}


