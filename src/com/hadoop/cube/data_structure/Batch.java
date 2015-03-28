package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hadoop.cube.utils.Utils;

public class Batch {
	public List<Cuboid> cuboids = new ArrayList<Cuboid>();
	public int partition_factor = 0;
	public List<Integer> lowestAttributes = new ArrayList<Integer>();
	public boolean isFriendly;
	
	public Batch(){
	}
	
	public boolean isFriendly(){
		return isFriendly;
	}
	
	public Batch(String str){
		String[] parts = str.split(",");
		partition_factor = Integer.parseInt(parts[0]);
		
		String[] cuboidStr = parts[1].split(";");
		cuboids = new ArrayList<Cuboid>();
		for(int i = 0; i < cuboidStr.length; i++){
			cuboids.add(new Cuboid(cuboidStr[i]));
		}
		
//		lowestAttributes = new ArrayList<Integer>();
//		String[] attrs = parts[2].split(";");
//		for(String attr: attrs){
//			lowestAttributes.add(Integer.parseInt(attr));
//		}
		
		if (parts[2].compareTo("0") == 0){	
			isFriendly = false;
		}else{
			isFriendly = true;
		}
	}
	
	public String convertToString() {
		String str = "";
		str = Integer.toString(partition_factor) + ",";
		for(int i = 0; i < cuboids.size() - 1; i++){
			str += cuboids.get(i).convertToString() + ";";
		}
		str += cuboids.get(cuboids.size() - 1).convertToString() + ",";
		//str += Utils.joinI(lowestAttributes, ";") + ",";
		if (isFriendly == false)
			str += "0";
		else
			str += "1";
		return str;
	};
	
	public void updateLowestAttributes(){
		lowestAttributes.clear();
		int max_level = 0;
		for(int i = 0; i < cuboids.size(); i++){
			Cuboid cuboid = cuboids.get(i);
			if (cuboid.level > max_level){
				max_level = cuboid.level;
			}
		}
		
		for(int i = 0; i < cuboids.size(); i++){
			Cuboid cuboid = cuboids.get(i);
			if (cuboid.level == max_level){
				List<Integer> numPresentation = cuboid.numPresentation;
				for(int j = 0; j < numPresentation.size(); j++){
					if (lowestAttributes.contains(numPresentation.get(j)) == false){
						lowestAttributes.add(numPresentation.get(j));
					}
				}
			}
		}
		
		Collections.sort(lowestAttributes);
	}
	
	public void print(){
		System.out.println("\n");
		String type = "Friendly Batch";
		if (isFriendly() == false)
			type = "Unfriendly Batch";
		
		System.out.print(type + ":\t(" + cuboids.get(0) + ")");
		
		for(int j = 1; j < cuboids.size(); j++)
			System.out.print("\t(" + cuboids.get(j) + ")");
		
		System.out.println("\n" + Utils.joinI(lowestAttributes, "-"));
		System.out.println("-------------------------------------------------");
		//BFS(batch.cuboids.get(0));
		
	}
}
