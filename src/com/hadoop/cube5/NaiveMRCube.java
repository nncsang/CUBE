package com.hadoop.cube5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.cube5.HashPartitioner;
import com.hadoop.cube.TimeStampWritable;
import com.hadoop.cube2rollups.Cube;
import com.hadoop.cube2rollups.GlobalSettings;
import com.hadoop.cube2rollups.Region;
import com.hadoop.cube2rollups.Utils;


public class NaiveMRCube extends Configured implements Tool {
	private int numReducers;
	private Path inputFile;
	private Path outputDir;
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new NaiveMRCube(args), args);
	    System.exit(res);
	}
	
	public NaiveMRCube(String[] args) {
	    if (args.length != 3) {
	      System.out.print(args.length);
	      System.out.println("Usage: NaiveMRCube <num_reducers> <input_path> <output_path>");
	      System.exit(0);
	    }
	    
	    this.numReducers = Integer.parseInt(args[0]);
	    this.inputFile = new Path(args[1]);
	    this.outputDir = new Path(args[2]);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		String[] attributes = {"y", "M", "d", "h", "m"};
		
		Cube cube = new Cube(attributes);
		Set<Region> regions = cube.cubeRegions();
		Iterator<Region> iter = regions.iterator();
		String regionList = "";
		while(iter.hasNext()){
			String region = iter.next().toString();
			//region = region.substring(1, region.length() - 1);
			regionList = regionList + region + GlobalSettings.DELIM_BETWEEN_CONTENTS_OF_TUPLE;
		}
		
		regionList = regionList.substring(0, regionList.length() - 1);
		
		conf.set("attributes", Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES));
		conf.set("regionList", regionList);
		
		//if file output is existed, delete it
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		    
		Job job = new Job(conf, "NaiveMRCube5Attr"); // TODO: define new job instead of null using conf
		
		// TODO: set job input format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		    
		// TODO: set map class and the map output key and value classes
		job.setMapperClass(NaiveMRCubeMapper.class);
		job.setMapOutputKeyClass(TimeStampWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		    
		// TODO: set reduce class and the reduce output key and value classes
		job.setReducerClass(NaiveMRCubeReducer.class);
		job.setOutputKeyClass(TimeStampWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		
		job.setCombinerClass(NaiveMRCubeReducer.class);
		
		// TODO: set job output format
		job.setOutputFormatClass(TextOutputFormat.class);
		    
		// TODO: add the input file as job input (from HDFS) to the variable
		//       inputFile
		FileInputFormat.addInputPath(job, inputFile);
		    
		// TODO: set the output path for the job results (to HDFS) to the variable
		//       outputPath
		FileOutputFormat.setOutputPath(job, outputDir);
		    
		// TODO: set the number of reducers using variable numberReducers
		job.setNumReduceTasks(numReducers);
		    
		    // TODO: set the jar class
		job.setJarByClass(NaiveMRCube.class);
		
		return job.waitForCompletion(true) ? 0 : 1; // this will execute the job		
	}
}

class NaiveMRCubeMapper extends Mapper<TimeStampWritable, LongWritable, TimeStampWritable, LongWritable>{
	private List<Region> regions;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] regionListString = conf.get("regionList").split(GlobalSettings.DELIM_BETWEEN_CONTENTS_OF_TUPLE);
		
		this.regions = new ArrayList<Region>();
		
		for(int i = 0; i < regionListString.length; i++){
			regions.add(new Region(regionListString[i].split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES)));
		}
	}
	
	@Override
	protected void map(TimeStampWritable value, LongWritable index, Context context)
			throws IOException, InterruptedException {
		
		int size = regions.size();
		for(int i = 0; i < size; i++){
			Region region = regions.get(i);
			String[] attributes = region.getAttributes();
			int length = attributes.length;
			
			TimeStampWritable key = new TimeStampWritable();
			for(int j = 0; j < length; j++){
				if (attributes[j].equals(GlobalSettings.ALL)){
					key.fields[j] = TimeStampWritable.NullValue;
				}else{
					key.fields[j] = value.fields[j];
				}
			}
			
			context.write(key, index);
		}
	}
}

class NaiveMRCubeReducer extends Reducer<TimeStampWritable, LongWritable, TimeStampWritable, LongWritable>{
	@Override
	protected void reduce(TimeStampWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable lw : values) {
		    sum += lw.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
