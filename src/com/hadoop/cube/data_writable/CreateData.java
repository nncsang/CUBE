package com.hadoop.cube.data_writable;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

public class CreateData {

	public static void main(String[] args) throws IOException {
		
		if (args.length != 3 && args.length != 4){
			System.out.println("Usage: <input> <output> <tuple_length> [limit]");
			return;
		}
		
		String[] input = args[0].split(",");
		String output = args[1];
		int length = Integer.valueOf(args[2]);
		int limit;
		
		try{
			limit = Integer.valueOf(args[3]);
		}catch(Exception ex){
			limit = -1;
		}
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		Path outFile = new Path(output);
		SequenceFile.Writer writer = null;
		
	
		String strLine ="";
		long current = 0;
		Tuple.setLength(length);
		Tuple key = new Tuple();
		
		LongWritable value = new LongWritable();
        writer = SequenceFile.createWriter(fs, conf, outFile, key.getClass(), value.getClass());
        
        for (int i = 0; i < input.length; i++){
        	System.out.println("-------------------------------------------");
        	System.out.println("Starting to write data of " + input[i]);
        	System.out.println("");
	        FileInputStream fstream = new FileInputStream(input[i]);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			
			while ((strLine = br.readLine()) != null)   {
				String[] attributes = strLine.split(",");
				
		        try{
		        	int year = Integer.parseInt(attributes[0]);
			        int month = Integer.parseInt(attributes[1]);
			        int dayOfMonth = Integer.parseInt(attributes[2]);
			        
			        int flightNumber = Integer.parseInt(attributes[9]);
			        int origin = attributes[16].hashCode();
			        int dest = attributes[17].hashCode();
			        int p = Integer.parseInt(attributes[18]);
			        
			        
			        key.set(year, month, dayOfMonth, flightNumber, origin, dest);
		            value.set(p);
		            writer.append(key, value);
		            
		            current++;
		            if (current % 1000000 == 0){
		            	System.out.println("Num of written tuples: " + current);
		            }
		            
		            if (current == limit && limit != -1)
		            	break;
		        }
		        catch(Exception ex){
		        	System.out.println(ex.toString());
		        }
			}
			
			br.close();
			fstream.close();
				
			if (current == limit && limit != -1)
            	break;
	    }
        
		
		IOUtils.closeStream(writer);
		
		System.out.println("");
		System.out.println("Total written tuples: " + current);
		System.out.println("Done");
	}
}
