package com.hadoop.cube.naive;

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

import com.hadoop.cube.data_structure.CubeLattice;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.irg_plus_irg.HashPartitioner;
import com.hadoop.cube.old_data_writable.AirlineWritable;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;


public class NaiveMRCube extends Configured implements Tool {
	private int numReducers;
	private Path inputFile;
	private Path outputDir;
	private int tupleLength;
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new NaiveMRCube(args), args);
	    System.exit(res);
	}
	
	public NaiveMRCube(String[] args) {
	    if (args.length != 4) {
	      System.out.println("Usage: NaiveMRCube <num_reducers> <input_path> <output_path> <tuple_length>");
	      System.exit(0);
	    }
	    
	    this.numReducers = Integer.parseInt(args[0]);
	    this.inputFile = new Path(args[1]);
	    this.outputDir = new Path(args[2]);
	    this.tupleLength = Integer.parseInt(args[3]);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
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
		
		conf.set("attributes", Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES));
		conf.set("regionList", regionList);
		
		//if file output is existed, delete it
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		    
		Job job = new Job(conf, "NaiveMRCube"); // TODO: define new job instead of null using conf
		
		// TODO: set job input format
		job.setInputFormatClass(TextInputFormat.class);
		    
		// TODO: set map class and the map output key and value classes
		job.setMapperClass(NaiveMRCubeMapper.class);
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(LongWritable.class);
		    
		// TODO: set reduce class and the reduce output key and value classes
		job.setReducerClass(NaiveMRCubeReducer.class);
		job.setOutputKeyClass(Tuple.class);
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

class NaiveMRCubeMapper extends Mapper<LongWritable, Text, Tuple, LongWritable>{
	private List<Cuboid> regions;
	private LongWritable sum = new LongWritable(0);
	private int[] data = new int[6];
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] regionListString = conf.get("regionList").split(GlobalSettings.DELIM_BETWEEN_CONTENTS_OF_TUPLE);
		
		this.regions = new ArrayList<Cuboid>();
		
		for(int i = 0; i < regionListString.length; i++){
			regions.add(new Cuboid(regionListString[i].split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES)));
		}
	}
	
	@Override
	protected void map(LongWritable index, Text line, Context context)
			throws IOException, InterruptedException {
		
		String[] values = line.toString().split("\t");
		
		for(int i = 0; i < Tuple.length; i++){
			data[i] = Integer.parseInt(values[i]);
		}
		
		sum.set(Integer.parseInt(values[Tuple.length]));
		
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
					key.fields[j] = data[j];
				}
			}
			
			context.write(key, sum);
		}
	}
}

class NaiveMRCubeReducer extends Reducer<Tuple, LongWritable, Tuple, LongWritable>{
	@Override
	protected void reduce(Tuple key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable lw : values) {
		    sum += lw.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
