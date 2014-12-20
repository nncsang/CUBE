package com.hadoop.cube;

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

import com.hadoop.cube.AirlineWritable;

public class CreateDataBin {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String input = args[0];
		String output = args[1];
		// TODO Auto-generated method stub
		System.out.println("Starting to write data");
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		Path outFile = new Path(output);
		SequenceFile.Writer writer = null;
		
		FileInputStream fstream = new FileInputStream(input);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		
		String strLine ="";
		int i = 0;
		
		AirlineWritable key = new AirlineWritable();
		LongWritable value = new LongWritable();
        writer = SequenceFile.createWriter(fs, conf, outFile, key.getClass(), value.getClass());
        
		while ((strLine = br.readLine()) != null)   {
			i++;
			
			String[] attributes = strLine.split(",");
			
	        try{
		        int month = Integer.parseInt(attributes[1]);
		        int dayOfMonth = Integer.parseInt(attributes[2]);
		        int dayOfWeek = Integer.parseInt(attributes[3]);;
		        int flightNumber = Integer.parseInt(attributes[9]);
		        int origin = attributes[16].hashCode();
		        int dest = attributes[17].hashCode();
		        int p = Integer.parseInt(attributes[18]);
		        
		        key.set(month, dayOfMonth, dayOfWeek, flightNumber, origin, dest);
	            value.set(p);
	            writer.append(key, value);
	        }
	        catch(Exception ex){
	        	
	        }
	        
	        if (i == 25000){
				break;
			}
		}
		
		IOUtils.closeStream(writer);
	    System.out.println("Done");
	}

}
