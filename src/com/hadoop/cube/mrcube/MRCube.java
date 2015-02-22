package com.hadoop.cube.mrcube;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.cube.buc.BUC;
import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.data_structure.CubeLattice;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.HeuristicBasedConverter;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Checker;
import com.hadoop.cube.utils.Utils;


public class MRCube extends Configured implements Tool{
	private int numReducers;
	private Path inputPath;
	private Path outputDir;
	private int tupleLength;
	
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRCube(args), args);
		System.exit(res);
	}
	
	public MRCube(String[] args) {
		if (args.length != 4) {
			System.out.println("Usage: MRCube <input_path> <output_path> <num_reducers> <tuple_length>");
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
		Job job = new Job(conf, "MRCube"); 
		
		// set job input format
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set map class and the map output key and value classes
		job.setMapperClass(MRCubeMapper.class);
		job.setMapOutputKeyClass(Segment.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(MRCubePartitioner.class);
		//job.setSortComparatorClass(IRGPlusIRGSorter.class);
		
		// set reduce class and the reduce output key and value classes
		job.setReducerClass(MRCubeReducer.class);
		
		//job.setSortComparatorClass(TimestampWritable.Comparator.class);

		// set job output format
		job.setOutputKeyClass(Tuple.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setCombinerClass(MRCubeCombiner.class);
		
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
		job.setJarByClass(MRCube.class);
		
		String[] attributes = new String[this.tupleLength];
			
		for(int i = 0; i < this.tupleLength; i++)
			attributes[i] = Integer.toString(i);

		CubeLattice cube = new CubeLattice(attributes);
		List<Cuboid> cuboids = cube.cuboids();
		
		/** for testing */
		cuboids.get(0).setFriendly(false);
		cuboids.get(2).setFriendly(false);
		cuboids.get(3).setFriendly(false);
		
		//cube.printCuboids();
		cube.batching();
		//cube.printBatches();
		
		String friendlyBatches = "";
		String unfriendlyBatches = "";
		
		for(int i = 0; i < cube.friendlyBatches.size() - 1; i++)
			friendlyBatches += cube.friendlyBatches.get(i).convertToString() + "=";
		friendlyBatches += cube.friendlyBatches.get(cube.friendlyBatches.size() - 1).convertToString();
		
		for(int i = 0; i < cube.unfriendlyBatches.size() - 1; i++)
			unfriendlyBatches += cube.unfriendlyBatches.get(i).convertToString() + "=";
		unfriendlyBatches += cube.unfriendlyBatches.get(cube.unfriendlyBatches.size() - 1).convertToString();
		
		List<BUC> bucs = new ArrayList<BUC>();
		
		List<List<Integer>> partitionOrder = new ArrayList<List<Integer>>();
		
		for(Batch batch: cube.friendlyBatches){
			BUC buc = new BUC(batch);
			//batch.print();
			//buc.print();
			//System.out.println(Utils.joinI(batch.cuboids.get(0).numPresentation, ""));
			bucs.add(buc);
			partitionOrder.add(batch.cuboids.get(0).numPresentation);
		}
		
		Segment.partitionOrder = partitionOrder;
		Segment.updateSortOrder();
		
		String bucsStr = "";
		for(int i = 0; i < bucs.size() - 1; i++){
			bucsStr += bucs.get(i).convertToString() + "z";
			//bucs.get(i).printSortSegments(bucs.get(i).sortSegments);
		}
		bucsStr += bucs.get(bucs.size() - 1).convertToString();
		
		
		job.getConfiguration().set("nBatch", Integer.toString(bucs.size()));
		job.getConfiguration().set("unfriendlyBatches", unfriendlyBatches);
		job.getConfiguration().set("bucsStr", bucsStr);
		
		job.waitForCompletion(true);
		Checker.main(null);
		return 0;
	}
}
