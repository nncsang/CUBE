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


public class Cube {
	private Set<Region> cubeRegions;
	private String[] attributes;
	private int numDim = 0;
	
	public Cube(String[] attributeNames){
		this.attributes = attributeNames;
		this.numDim = attributeNames.length;
	}
	
	public Set<Region> cubeRegions(){
		if (this.cubeRegions != null && this.cubeRegions.isEmpty())
			this.cubeRegions.clear();
		else
			this.cubeRegions = new HashSet<Region>();
	
		/*
		List<Integer> mask = new ArrayList<Integer>();
		for(int i = 0; i < this.numDim; i++)
			mask.add(0);
		
		computeCubeRegions(0, 0, 0, mask);
		*/
		computeCubeRegions();
		return this.cubeRegions;
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
		
		this.cubeRegions.add(new Region(region.toArray(new String[region.size()])));
	}
	
	public void printRegions(){
		Iterator<Region> iter = this.cubeRegions.iterator();
		while(iter.hasNext()){
			System.out.println(iter.next().toString());
		}
	}
	
	public List<RollUp> toRollUps(CubeConverter converter, int pivot){
		return converter.toRollUps(this.attributes, pivot);
	}
	
	public List<RollUp> toRollUps(CubeConverter converter, Set<Region> regions, int pivot){
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
			
			List<String> region = new ArrayList<String>();
			for(int j = 0; j < binary.length(); j++){
				if (binary.charAt(j) == '0'){
					region.add(GlobalSettings.ALL);
				}else{
					region.add(this.attributes[j]);
				}
			}
			
			this.cubeRegions.add(new Region(region.toArray(new String[region.size()])));
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
