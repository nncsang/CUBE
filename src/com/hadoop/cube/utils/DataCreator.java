package com.hadoop.cube.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;


public class DataCreator {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if (args.length != 3){
			System.out.println("DataCreator <output_file> <data_size> <number_of_year>");
			return;
		}
			
		System.out.println("Starting to write data");
		PrintWriter out = new PrintWriter(args[0]);
		Random random = new Random();
		long dataSize = Long.parseLong(args[1]) * 1024 * 1024;
		long noYear = Integer.parseInt(args[2]);
		long oneGB = 1024 * 1024;
		String writeData = "";
		long cnt = 0;
		while (true) {
			long y = random.nextInt(1000000000) % noYear + 1990;
			long M = random.nextInt(1000000000) % 12 + 1;
			long d = random.nextInt(1000000000) % 31 + 1;
			long h = random.nextInt(1000000000) % 24;
			long m = random.nextInt(1000000000) % 60;
			long s = random.nextInt(1000000000) % 60;
			long p = random.nextInt(1000000000) % 100 + 1;
			writeData = y + "\t" + M + "\t" + d + "\t" + h + "\t" + m + "\t" + s + "\t" + p;
			cnt += writeData.length() + 2;
			if (cnt >= dataSize) {
				break;
			}
			out.println(writeData);
			if (cnt % oneGB < writeData.length() + 2) {
				System.out.println((cnt)/oneGB + "GB");
			}
			
		}
		out.close();
		System.out.println("Done");
	}

}
