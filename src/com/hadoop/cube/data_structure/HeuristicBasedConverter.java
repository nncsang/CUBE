package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hadoop.cube.settings.GlobalSettings;

public class HeuristicBasedConverter implements CubeConverter{
	Set<Region> cubeRegions;
	
	RollUp rollup;
	List<RollUp> rollupList;
	List<Region> rollupRegions;
	
	String[] attributes;
	Map<String, Integer> result;
	Map<String, Integer> previousResult;
	Set<String> prefixSet;
	int pivot = -1;
	
	public HeuristicBasedConverter(){
		this.rollupList = new ArrayList<RollUp>();
	}
	
	public List<RollUp> compute(String[] attributes, int pivot){
		updateCubeRegions();
		
		while(this.cubeRegions.isEmpty() == false){
			// Next rollup
			frequencies();
			List<String> prefix = new ArrayList<String>();
			
			String maxAttribute = "";
			int max = -1;
			
			for(int i = 0; i < attributes.length; i++){
				int value = this.result.get(attributes[i]);
				if (value > max){
					max = value;
					maxAttribute = attributes[i];
				}
			}
			
			prefix.add(maxAttribute);
			//this.previousResult = new HashMap<String, Integer>(this.result);
			
			for(int i = 1; i < attributes.length - 1; i++){
				frequenciesByPrefix(prefix);
				
				maxAttribute = "";
				max = -1;
				
				for(int j = 0; j < attributes.length; j++){
					if (this.prefixSet.contains(attributes[j]))
						continue;
					
					int value = this.result.get(attributes[j]);
					if (value > max){
						max = value;
						maxAttribute = attributes[j];
					}
					
					// Need consider
//					else if (value == max){
//						if (this.previousResult.get(maxAttribute) < previousResult.get(attributes[j])){
//							max = value;
//							maxAttribute = attributes[j];
//						}
//					}
				}
				
				//this.previousResult = new HashMap<String, Integer>(this.result);
				prefix.add(maxAttribute);
			}
			
			updatePrefixSet(prefix);
			
			for(int i = 0; i < attributes.length; i++){
				if (this.prefixSet.contains(attributes[i]))
					continue;
				else
					prefix.add(attributes[i]);
			}
			
			this.rollup = new RollUp(prefix.toArray(new String[prefix.size()]));
			this.rollupRegions = this.rollup.rollupRegions();
			
			updateCubeRegions();
		}
		
		return this.rollupList;
	}
	
	@Override
	public List<RollUp> toRollUps(String[] attributes, int pivot) {
		this.pivot = pivot;
		this.attributes = attributes;
		
		this.cubeRegions = new Cube(attributes).cubeRegions();
		this.rollup = new RollUp(attributes);
		this.rollupRegions = this.rollup.rollupRegions();
		
		return compute(attributes, pivot);
	}
	
	public void updateCubeRegions(){
		if (this.pivot != -1){
			rollup.isNeedEmitTuple[0] = false;
			rollup.isNeedEmitTuple[1] = false;
		}
		
		for(int i = 0; i < this.rollupRegions.size(); i++){
			Region region = this.rollupRegions.get(i);
			if (checkAndRemoveRegion(region)){
				this.rollup.enabledRegions.add(i);
				if (i <= pivot){
					rollup.isNeedEmitTuple[0] = true;
				}else{
					rollup.isNeedEmitTuple[1] = true;
				}
			}
		}
		
		this.rollupList.add(this.rollup);
	}
	
	public boolean checkAndRemoveRegion(Region regionToRemove){
		Map<String, Integer> map1 = new HashMap<String, Integer>();
		Map<String, Integer> map2 = new HashMap<String, Integer>();
		
		map1.put("*", 0);
		for(int i = 0; i < this.attributes.length; i++){
			map1.put(this.attributes[i], 0);
		}
		
		String[] attributes1 = regionToRemove.getAttributes();
		for(int i = 0; i < attributes1.length; i++){
			map1.put(attributes1[i], map1.get(attributes1[i]) + 1);
		}
		
		Iterator<Region> iter = this.cubeRegions.iterator();
		while(iter.hasNext()){
			Region region = iter.next();
			
			String[] attributes2 = region.getAttributes();
			
			map2.put("*", 0);
			for(int i = 0; i < this.attributes.length; i++){
				map2.put(this.attributes[i], 0);
			}
			
			for(int i = 0; i < attributes2.length; i++){
				map2.put(attributes2[i], map2.get(attributes2[i]) + 1);
			}
			
			if (map1.equals(map2)){
				this.cubeRegions.remove(region);
				return true;
			}
		}
		return false;
	}
	
	Map<String, Integer> frequencies(){
		this.resetFrequencyMap();
		Iterator<Region> iter = this.cubeRegions.iterator();
		
		while(iter.hasNext()){
			String[] attributes = iter.next().getAttributes();
			for(int i = 0; i < attributes.length; i++){
				if (attributes[i].equals(GlobalSettings.ALL))
					continue;
				this.result.put(attributes[i], this.result.get(attributes[i]) + 1);
			}
		}
		
		return this.result;
	}
	
	Map<String, Integer> frequenciesByPrefix(List<String> prefix){
		this.resetFrequencyMap();
		
		Iterator<Region> iter = this.cubeRegions.iterator();
		
		this.prefixSet = new HashSet<String>();
		for(int i = 0; i < prefix.size(); i++)
			this.prefixSet.add(prefix.get(i));
		
		while(iter.hasNext()){
			String[] attributes = iter.next().getAttributes();
			
			if (checkContain(attributes, this.prefixSet)){
				for(int i = 0; i < attributes.length; i++){
					
					if (attributes[i].equals(GlobalSettings.ALL))
						continue;
					
					if (this.prefixSet.contains(attributes[i]))
						continue;
					
					this.result.put(attributes[i], this.result.get(attributes[i]) + 1);
				}
			}
		}
		
		return this.result;
	}
	
	public boolean checkContain(String[] attributes, Set<String> prefixSet){
		Set<String> attributeSet = new HashSet<String>();
		
		for(int i = 0; i < attributes.length; i++)
			attributeSet.add(attributes[i]);
		
		if (attributeSet.containsAll(prefixSet))
			return true;
		
		return false;
	}
	
	public void resetFrequencyMap(){
		this.result = new HashMap<String, Integer>();
		
		for(int i = 0; i < this.attributes.length; i++)
			this.result.put(this.attributes[i], 0);
	}
	
	public void updatePrefixSet(List<String> prefix){
		this.prefixSet = new HashSet<String>();
		for(int i = 0; i < prefix.size(); i++)
			this.prefixSet.add(prefix.get(i));
	}

	@Override
	public List<RollUp> toRollUps(String[] attributes, Set<Region> regions,
			int pivot) {
		this.pivot = pivot;
		this.attributes = attributes;
		
		this.cubeRegions = regions;
		this.rollup = new RollUp(attributes);
		this.rollupRegions = this.rollup.rollupRegions();
		
		return compute(attributes, pivot);
	}
}
