package com.hadoop.cube2rollups;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class RollUp {
	private String[] attributes;
	public List<Region> rollupRegions;
	private int numDim;
	public List<Integer> enabledRegions;
	public boolean[] isNeedEmitTuple = {true, true};
	
	public RollUp(String[] attributes){
		this.attributes = attributes;
		this.numDim = attributes.length;
		
		this.enabledRegions = new ArrayList<Integer>();
	}
	
	public String[] getAttributes(){
		return this.attributes;
	}
	
	List<Region> rollupRegions(){
		this.rollupRegions = new ArrayList<Region>();
		
		for(int i = 0; i < this.numDim; i++){
			List<String> region = new ArrayList<String>();
			
			for(int j = 0; j < i; j++)
				region.add(this.attributes[j]);
			
			for(int j = i; j < this.numDim; j++)
				region.add(GlobalSettings.ALL);
			
			this.rollupRegions.add((new Region(region.toArray(new String[region.size()]))));
		}
		
		this.rollupRegions.add((new Region(this.attributes)));
		return this.rollupRegions;
	}
	
	public void printRegions(){
		Iterator<Region> iter = this.rollupRegions.iterator();
		while(iter.hasNext()){
			System.out.println(iter.next().toString());
		}
	}
	
	@Override
	public String toString() {
		return Utils.join(this.attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
	}
	
}
