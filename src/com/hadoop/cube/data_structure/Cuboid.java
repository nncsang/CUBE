package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.List;

import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class Cuboid implements Comparable<Cuboid>{
	public String[] attributes;
	public String string;
	public int numDim;
	public List<Cuboid> children;
	public List<Cuboid> parents;
	public boolean isFriendly;
	public int partition_factor;
	public boolean isBatched;
	public int level;
	public List<Integer> numPresentation;
	
	public Cuboid(String str){
		String[] parts = str.split("a");
		
		level = Integer.parseInt(parts[0]);
		
		//System.out.println(parts[1]);
		String[] atts = parts[1].split("_");
		attributes = new String[atts.length];
		for(int i = 0; i < attributes.length; i++)
			attributes[i] = atts[i];
		this.string = Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
		
		if (parts.length >= 3){
			String[] representation = parts[2].split("_");
			numPresentation = new ArrayList<Integer>();
			
			for(String num: representation){
				numPresentation.add(Integer.parseInt(num));
			}
		}
	}
	
	public String convertToString(){
		String str = "";
		str += Integer.toString(level) + "a";
		str += Utils.join(attributes, "_") + "a";
		str += Utils.joinI(numPresentation, "_") + "a";
		return str;
	}
	
	public Cuboid(String[] attributes){
		this.attributes = attributes;
		this.numDim = attributes.length;
		this.children = new ArrayList<Cuboid>();
		this.parents = new ArrayList<Cuboid>();
		this.string = Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
		this.isFriendly = true;
		this.partition_factor = 0;
		this.isBatched = false;
		this.level = -1;
		this.numPresentation = new ArrayList<Integer>();
		
		for(int i = 0; i < this.attributes.length; i++)
			if (this.attributes[i].compareTo(GlobalSettings.ALL) != 0)
				numPresentation.add(i);
	}
	
	@Override
	public String toString() {
		return this.string;
	}
	
	public int hashCodeWithoutPosition(){
		int hashCode = 0;
		for(int i = 0; i < this.attributes.length; i++){
			hashCode += this.attributes[i].hashCode();
		}
		
		return hashCode;
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	@Override
	public boolean equals(Object arg0) {
		if (this.compareTo((Cuboid) arg0) == 0)
			return true;
		else
			return false;
	}

	public String[] getAttributes(){
		return this.attributes;
	}
	
	public List<Cuboid> getChildren(){
		return this.children;
	}
	
	public List<Cuboid> getParents(){
		return this.parents;
	}
	
	public boolean isFriendly(){
		return this.isFriendly;
	}
	
	public void setFriendly(boolean friendly){
		this.isFriendly = friendly;
	}
	
	public void setPartitionFactor(int fac){
		this.partition_factor = fac;
	}
	
	public int getPartitionFactor(){
		return this.partition_factor;
	}
	
	@Override
	public int compareTo(Cuboid cuboid) {
		if (this.numDim != cuboid.numDim){
			if (this.numDim > cuboid.numDim)
				return 1;
			else
				return -1;
		}
		
		for(int i = 0; i < this.numDim; i++){
			int cmp = this.attributes[i].compareTo(cuboid.attributes[i]); 
			if (cmp != 0)
				return cmp;
		}
		
		return 0;
	}
}
