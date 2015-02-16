package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.hadoop.cube.settings.GlobalSettings;


public class CubeLattice {
	private List<Cuboid> cuboids;
	private String[] attributes;
	private int numDim = 0;
	
	public CubeLattice(String[] attributeNames){
		this.attributes = attributeNames;
		this.numDim = attributeNames.length;
	}
	
	public List<Cuboid> cuboids(){
		if (this.cuboids != null && this.cuboids.isEmpty())
			this.cuboids.clear();
		else
			this.cuboids = new ArrayList<Cuboid>();
	
		/*
		List<Integer> mask = new ArrayList<Integer>();
		for(int i = 0; i < this.numDim; i++)
			mask.add(0);
		
		computeCubeRegions(0, 0, 0, mask);
		*/
		computeCubeRegions();
		return this.cuboids;
	}
	
	public void addRegion(List<Integer> mask){
		Iterator<Integer> itor = mask.iterator();
		List<String> region = new ArrayList<String>();
		
		int index = 0;
		while(itor.hasNext()){
			if (itor.next() == 0)
				region.add(GlobalSettings.ALL);
			else
				region.add(this.attributes[index]);
			index++;
		}
		
		this.cuboids.add(new Cuboid(region.toArray(new String[region.size()])));
	}
	
	public void printCuboids(){
		for(int i = 0; i < this.cuboids.size(); i++){
			Cuboid cuboid = cuboids.get(i);
			System.out.println(cuboid);
		}
	}
	
	public List<RollUp> toRollUps(CubeConverter converter, int pivot){
		return converter.toRollUps(this.attributes, pivot);
	}
	
	public List<RollUp> toRollUps(CubeConverter converter, List<Cuboid> regions, int pivot){
		return converter.toRollUps(this.attributes, regions, pivot);
	}
	
	private void computeCubeRegions(){
		int nRegion = (int) Math.pow(2, this.numDim);
		for(int i = 0; i < nRegion; i++){
			String binary = Integer.toBinaryString(i);
			
			int repeatTimes = this.numDim - binary.length();
			String temp = "";
			for(int j = 0; j < repeatTimes; j++)
				temp += "0";
			binary = temp + binary;
			
			List<String> cuboid = new ArrayList<String>();
			for(int j = 0; j < binary.length(); j++){
				if (binary.charAt(j) == '0'){
					cuboid.add(GlobalSettings.ALL);
				}else{
					cuboid.add(this.attributes[j]);
				}
			}
			
			this.cuboids.add(new Cuboid(cuboid.toArray(new String[cuboid.size()])));
		}
		
		for(int i = 0; i < this.cuboids.size(); i++){
			int value_of_parent = 0;
			
			Cuboid cuboid = cuboids.get(i);
			String[] attributes = cuboid.attributes;
			int length = attributes.length;
			
			List<Integer> index_of_zeros = new ArrayList<Integer>();
			
			for(int j = 0; j < length; j++){
				if (attributes[j].compareTo(GlobalSettings.ALL) != 0){
					value_of_parent += (int) Math.pow(2, length - j - 1);
				}else
					index_of_zeros.add(j);
			}
			
			for(int j = 0; j < index_of_zeros.size(); j++){
				cuboid.children.add(cuboids.get(value_of_parent + (int) Math.pow(2, length - index_of_zeros.get(j) - 1)));
			}
		}
	}
	
	/** stupid version **/
	private void computeCubeRegions(int mainOrigin, int origin, int dim, List<Integer> mask){
		addRegion(mask);
		
		for(int i = dim; i < this.numDim; i++){
			if (mainOrigin == 0 && i == 0){
				mask.set(0, 1);
				addRegion(mask);
			}
			
			for(int j = i + 1; j < this.numDim; j++){
				mask.set(j, 1);
				computeCubeRegions(-1, origin, j, mask);
				mask.set(j, 0);
			}
			
			if (mainOrigin == 0){
				origin = i + 1;
				if (origin < this.numDim){
					for(int k = 0; k < this.numDim; k++)
						mask.set(k, 0);
					mask.set(origin, 1);
					addRegion(mask);
				}
			}
		}
	}
}
