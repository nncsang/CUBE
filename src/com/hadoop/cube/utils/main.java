package com.hadoop.cube.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;

import com.hadoop.cube.buc.BUC;
import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.data_structure.CubeLattice;
import com.hadoop.cube.data_structure.HeuristicBasedConverter;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.data_writable.Tuple;

public class main {
	static public void main(String[] args){
		int pivot = 3;
		String[] attributes = {"A", "B", "C", "D"};
		//String[] attributes = {"1", "2", "3", "4", "5", "6"};
		//String[] attributes = {"y", "m", "d", "h", "mm"};
		//String[] attributes = {"y", "m", "d", "h"};
		//String[] attributes = {"A", "B", "C"};
		CubeLattice cube = new CubeLattice(attributes);
		List<Cuboid> cuboids = cube.cuboids();
		cuboids.get(0).setFriendly(false);
		cuboids.get(0).setPartitionFactor(10);
		cuboids.get(1).setFriendly(false);
		cuboids.get(4).setFriendly(false);
		cuboids.get(8).setFriendly(false);
		
		cube.printCuboids();
		cube.batching();
		cube.printBatches();
		
		Tuple.setLength(4);
		BUC buc = new BUC(cube.friendlyBatches.get(2));
		Tuple tuple1 = new Tuple(1,1,2,3);
		Tuple tuple2 = new Tuple(1,1,2,3);
		Tuple tuple3 = new Tuple(1,2,2,3);
		
		buc.addTuple(tuple1, new LongWritable(2));
		buc.addTuple(tuple2, new LongWritable(3));
		buc.addTuple(tuple3, new LongWritable(4));
		buc.finish();
		
		//cube.printStructure();
		
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
