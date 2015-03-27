package com.hadoop.cube.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Checker {
	static public void main(String[] args) throws IOException{
		Set<String> standardOutput = new HashSet<String>();
		Set<String> doubleOrWrong = new HashSet<String>();
		Set<String> doubleSet = new HashSet<String>();
		Set<String> missedSet = new HashSet<String>();
		
		String standardOutputFileName = "output_naive//part-r-00000";
		String outputFileName = "output_mrcube//part-r-00000";
		
		FileInputStream fstream = new FileInputStream(standardOutputFileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		
		String strLine;
		while ((strLine = br.readLine()) != null)   {
		  standardOutput.add(strLine);
		}
		
		br.close();
		
		
		fstream = new FileInputStream(outputFileName);
		br = new BufferedReader(new InputStreamReader(fstream));
		
		System.out.println("Ground Truth size:\t" + standardOutput.size());
		int size = 0;
		int numDoubleOrWrong = 0;
		
		while ((strLine = br.readLine()) != null)   {
			size++;
			if (standardOutput.contains(strLine)){
				standardOutput.remove(strLine);
			}else{
				doubleOrWrong.add(strLine);
				numDoubleOrWrong++;
			}
		}
		
		
		System.out.println("My output size:\t\t" + size);
		System.out.println("Double or Wrong:\t" + numDoubleOrWrong);
		br.close();
		
		System.out.println("Size left:\t\t" + standardOutput.size());
		int numDouble = numDoubleOrWrong - standardOutput.size();
		System.out.println("Double:\t\t\t" +  numDouble);
		
		Iterator<String> iter = standardOutput.iterator();
		while(iter.hasNext()){
			missedSet.add(iter.next());
		}
		
		iter = doubleOrWrong.iterator();
		while(iter.hasNext()){
			String item = iter.next();
			if (missedSet.contains(item) == false){
				doubleSet.add(item);
			}
		}
		
		Utils.headSet(doubleSet, 10);
	}
}
