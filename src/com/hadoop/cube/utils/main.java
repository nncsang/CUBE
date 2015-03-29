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
		Tuple.setLength(6);
		String[] attributes = {"A", "B", "C", "D", "E", "F"};
		//String[] attributes = {"1", "2", "3", "4", "5", "6"};
		//String[] attributes = {"y", "m", "d", "h", "mm"};
		//String[] attributes = {"y", "m", "d", "h"};
		//String[] attributes = {"A", "B", "C"};
		CubeLattice cube = new CubeLattice(attributes);
		List<Cuboid> cuboids = cube.cuboids();
//		cuboids.get(29).setFriendly(false);
//		cuboids.get(46).setFriendly(false);
//		cuboids.get(39).setFriendly(false);
//		cuboids.get(51).setFriendly(false);
//		cuboids.get(45).setFriendly(false);
//		cuboids.get(30).setFriendly(false);
//		cuboids.get(11).setFriendly(false);
//		cuboids.get(58).setFriendly(false);
//		cuboids.get(54).setFriendly(false);
//		cuboids.get(59).setFriendly(false);
//		cuboids.get(53).setFriendly(false);
//		cuboids.get(57).setFriendly(false);
		
		cuboids.get(0).setFriendly(false);
		cuboids.get(0).setPartitionFactor(5);
		
		cuboids.get(1).setFriendly(false);
		cuboids.get(1).setPartitionFactor(5);
		
		cuboids.get(2).setFriendly(false);
		cuboids.get(2).setPartitionFactor(5);
		
		cuboids.get(3).setFriendly(false);
		cuboids.get(3).setPartitionFactor(5);
		
		cuboids.get(4).setFriendly(false);
		cuboids.get(4).setPartitionFactor(5);
		
		cuboids.get(5).setFriendly(false);
		cuboids.get(8).setPartitionFactor(5);
		
		cuboids.get(9).setFriendly(false);
		cuboids.get(9).setPartitionFactor(5);
		
		cuboids.get(16).setFriendly(false);
		cuboids.get(16).setPartitionFactor(5);
		
		cuboids.get(17).setFriendly(false);
		cuboids.get(17).setPartitionFactor(5);
		
		//cuboids.get(1).setFriendly(false);
		
		cube.printCuboids();
		cube.printChildren(cuboids);
//		cube.batching();
//		cube.printBatches();
		
		
//		BUC buc = new BUC(cube.friendlyBatches.get(0));
//		buc.print();
//		List<Tuple> tuples = new ArrayList<Tuple>();
		
//		tuples.add(new Tuple(1987,	-1,	14,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	15,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	17,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	18,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	14,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	15,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	17,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	18,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	19,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	21,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	22,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	23,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	24,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	10,	25,	1451,	81856,	82012));
		
//		tuples.add(new Tuple(1987,	-1,	21,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	22,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	23,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	24,	1451,	81856,	82012));
//		tuples.add(new Tuple(1987,	-1,	25,	1451,	81856,	82012));
		
		
//		for(Tuple tuple: tuples){
//			buc.addTuple(tuple, 1);
//		}
//		
//		buc.finish();
		
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
