package com.hadoop.cube.data_structure;

import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.utils.Utils;

public class Region implements Comparable<Region>{
	String[] attributes;
	String string;
	int numDim;
	 
	public Region(String[] attributes){
		this.attributes = attributes;
		this.numDim = attributes.length;
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
		Region region = (Region) arg0;
		
		if (region.numDim != this.numDim)
			return false;
		
		if (this.hashCode() == region.hashCode())
			return true;
		
		return false;
	}

	public String[] getAttributes(){
		return this.attributes;
	}
	
	@Override
	public int compareTo(Region region) {
		if (this.numDim != region.numDim){
			if (this.numDim > region.numDim)
				return 1;
			else
				return -1;
		}
		
		if (this.hashCode() == region.hashCode())
			return 0;
		
		for(int i = 0; i < this.numDim; i++){
			int cmp = this.attributes[i].compareTo(region.attributes[i]); 
			if (cmp != 0)
				return cmp;
		}
		
		return 0;
	}
}
