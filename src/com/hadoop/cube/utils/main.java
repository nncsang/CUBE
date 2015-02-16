package com.hadoop.cube.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.hadoop.cube.data_structure.CubeLattice;
import com.hadoop.cube.data_structure.HeuristicBasedConverter;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;

public class main {
	static public void main(String[] args){
		int pivot = 3;
		String[] attributes = {"country", "state", "city", "topic"};
		//String[] attributes = {"1", "2", "3", "4", "5", "6"};
		//String[] attributes = {"y", "m", "d", "h", "mm"};
		//String[] attributes = {"y", "m", "d", "h"};
		//String[] attributes = {"A", "B", "C"};
		CubeLattice cube = new CubeLattice(attributes);
		List<Cuboid> cuboids = cube.cuboids();
		
		for(int i = 0; i < cuboids.size(); i++){
			Cuboid cuboid = cuboids.get(i);
			List<Cuboid> children = cuboid.getChildren();
			System.out.println(cuboid);
			System.out.println("Children: ");
			for(int j = 0; j < children.size(); j++){
				System.out.println(children.get(j));
			}
			
			System.out.println("---------------------------------------\n");
		}
		
//		cube.cubeRegions();
//		cube.printRegions();
//		
//		System.out.println("       ");
		
		
//		List<Cuboid> regions = new ArrayList<Cuboid>();
//		regions.add(new Cuboid(new String[]{"*", "*", "*", "*", "*", "*"}));
//		
//		regions.add(new Cuboid(new String[]{"country", "*", "*", "*", "*", "*"}));
//		regions.add(new Cuboid(new String[]{"*", "*", "*", "topic", "*", "*"}));
//		
//		regions.add(new Cuboid(new String[]{"*", "*", "*", "topic", "cat", "*"}));
//		regions.add(new Cuboid(new String[]{"country", "*", "*", "topic", "*", "*"}));
//		regions.add(new Cuboid(new String[]{"country", "state", "*", "*", "*", "*"}));
//		
//		regions.add(new Cuboid(new String[]{"*", "*", "*", "topic", "cat", "subcat"}));
//		regions.add(new Cuboid(new String[]{"country", "*", "topic", "cat", "*", "*"}));
//		regions.add(new Cuboid(new String[]{"country", "state", "*", "topic", "*", "*"}));
//		regions.add(new Cuboid(new String[]{"country", "state", "city", "*", "*", "*"}));
//		
//		regions.add(new Cuboid(new String[]{"country", "*", "*", "topic", "cat", "subcat"}));
//		regions.add(new Cuboid(new String[]{"country", "state", "*", "topic", "cat", "*"}));
//		regions.add(new Cuboid(new String[]{"country", "state", "city", "topic", "*", "*"}));
//		
//		regions.add(new Cuboid(new String[]{"country", "state", "*", "topic", "cat", "subcat"}));
//		regions.add(new Cuboid(new String[]{"country", "state", "city", "topic", "cat", "*"}));
//		
//		regions.add(new Cuboid(new String[]{"country", "state", "city", "topic", "cat", "subcat"}));
//		
//		System.out.println(regions.size());
//		
//		//List<RollUp> rollups = cube.toRollUps(new HeuristicBasedConverter(), pivot);
//		List<RollUp> rollups = cube.toRollUps(new HeuristicBasedConverter(), regions, pivot);
//		for(int i = 0; i < rollups.size(); i++){
//			System.out.println(i);
//			RollUp rollup = rollups.get(i);
//			System.out.println(rollups.get(i));
//			
//			Iterator<Integer> iter = rollup.enabledRegions.iterator();
//			while(iter.hasNext()){
//				System.out.println(rollup.rollupRegions.get(iter.next()));
//			}
//			
//			System.out.println(rollup.isNeedEmitTuple[0] + "\t" + rollup.isNeedEmitTuple[1]);
//			
//			iter = rollup.enabledRegions.iterator();
//			while(iter.hasNext()){
//				System.out.print(iter.next() + "\t");
//			}
//			
//			System.out.println("\n");
//			System.out.println("\n");
//		}
//		
//		System.out.println(rollups.size());
	}
}
