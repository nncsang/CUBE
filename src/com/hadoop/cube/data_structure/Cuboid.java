package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.List;

import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class Cuboid implements Comparable<Cuboid>{
	String[] attributes;
	String string;
	int numDim;
	List<Cuboid> children;
	 
	public Cuboid(String[] attributes){
		this.attributes = attributes;
		this.numDim = attributes.length;
		this.children = new ArrayList<Cuboid>();
		this.string = Utils.join(attributes, GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
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
		Cuboid cuboid = (Cuboid) arg0;
		
		if (cuboid.numDim != this.numDim)
			return false;
		
		if (this.hashCode() == cuboid.hashCode())
			return true;
		
		return false;
	}

	public String[] getAttributes(){
		return this.attributes;
	}
	
	public List<Cuboid> getChildren(){
		return this.children;
	}
	
	@Override
	public int compareTo(Cuboid cuboid) {
		if (this.numDim != cuboid.numDim){
			if (this.numDim > cuboid.numDim)
				return 1;
			else
				return -1;
		}
		
		if (this.hashCode() == cuboid.hashCode())
			return 0;
		
		for(int i = 0; i < this.numDim; i++){
			int cmp = this.attributes[i].compareTo(cuboid.attributes[i]); 
			if (cmp != 0)
				return cmp;
		}
		
		return 0;
	}
}
