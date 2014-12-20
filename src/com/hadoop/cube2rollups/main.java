package com.hadoop.cube2rollups;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class main {
	static public void main(String[] args){
		int pivot = 3;
		String[] attributes = {"country", "state", "city", "topic", "cat", "subcat"};
		//String[] attributes = {"1", "2", "3", "4", "5", "6"};
		//String[] attributes = {"y", "m", "d", "h", "mm"};
		//String[] attributes = {"y", "m", "d", "h"};
		//String[] attributes = {"A", "B", "C"};
		Cube cube = new Cube(attributes);
		
//		cube.cubeRegions();
//		cube.printRegions();
//		
//		System.out.println("       ");
		
		
		Set<Region> regions = new HashSet<Region>();
		regions.add(new Region(new String[]{"*", "*", "*", "*", "*", "*"}));
		
		regions.add(new Region(new String[]{"country", "*", "*", "*", "*", "*"}));
		regions.add(new Region(new String[]{"*", "*", "*", "topic", "*", "*"}));
		
		regions.add(new Region(new String[]{"*", "*", "*", "topic", "cat", "*"}));
		regions.add(new Region(new String[]{"country", "*", "*", "topic", "*", "*"}));
		regions.add(new Region(new String[]{"country", "state", "*", "*", "*", "*"}));
		
		regions.add(new Region(new String[]{"*", "*", "*", "topic", "cat", "subcat"}));
		regions.add(new Region(new String[]{"country", "*", "topic", "cat", "*", "*"}));
		regions.add(new Region(new String[]{"country", "state", "*", "topic", "*", "*"}));
		regions.add(new Region(new String[]{"country", "state", "city", "*", "*", "*"}));
		
		regions.add(new Region(new String[]{"country", "*", "*", "topic", "cat", "subcat"}));
		regions.add(new Region(new String[]{"country", "state", "*", "topic", "cat", "*"}));
		regions.add(new Region(new String[]{"country", "state", "city", "topic", "*", "*"}));
		
		regions.add(new Region(new String[]{"country", "state", "*", "topic", "cat", "subcat"}));
		regions.add(new Region(new String[]{"country", "state", "city", "topic", "cat", "*"}));
		
		regions.add(new Region(new String[]{"country", "state", "city", "topic", "cat", "subcat"}));
		
		System.out.println(regions.size());
		
		//List<RollUp> rollups = cube.toRollUps(new HeuristicBasedConverter(), pivot);
		List<RollUp> rollups = cube.toRollUps(new HeuristicBasedConverter(), regions, pivot);
		for(int i = 0; i < rollups.size(); i++){
			System.out.println(i);
			RollUp rollup = rollups.get(i);
			System.out.println(rollups.get(i));
			
			Iterator<Integer> iter = rollup.enabledRegions.iterator();
			while(iter.hasNext()){
				System.out.println(rollup.rollupRegions.get(iter.next()));
			}
			
			System.out.println(rollup.isNeedEmitTuple[0] + "\t" + rollup.isNeedEmitTuple[1]);
			
			iter = rollup.enabledRegions.iterator();
			while(iter.hasNext()){
				System.out.print(iter.next() + "\t");
			}
			
			System.out.println("\n");
			System.out.println("\n");
		}
		
		System.out.println(rollups.size());
	
	}
}
