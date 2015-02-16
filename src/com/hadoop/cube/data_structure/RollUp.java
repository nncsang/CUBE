package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class RollUp {
	private String[] attributes;
	public List<Cuboid> rollupRegions;
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
	
	List<Cuboid> rollupRegions(){
		this.rollupRegions = new ArrayList<Cuboid>();
		
		for(int i = 0; i < this.numDim; i++){
			List<String> region = new ArrayList<String>();
			
			for(int j = 0; j < i; j++)
				region.add(this.attributes[j]);
			
			for(int j = i; j < this.numDim; j++)
				region.add(GlobalSettings.ALL);
			
			this.rollupRegions.add((new Cuboid(region.toArray(new String[region.size()]))));
		}
		
		this.rollupRegions.add((new Cuboid(this.attributes)));
		return this.rollupRegions;
	}
	
	public void printRegions(){
		Iterator<Cuboid> iter = this.rollupRegions.iterator();
		while(iter.hasNext()){
			System.out.println(iter.next().toString());
		}
	}
	
	@Override
	public String toString() {
		return Utils.join(this.attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
	}
	
}
