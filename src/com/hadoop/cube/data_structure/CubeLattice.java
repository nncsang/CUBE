package com.hadoop.cube.data_structure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.hadoop.cube.settings.GlobalSettings;


public class CubeLattice {
	private List<Cuboid> cuboids;
	private String[] attributes;
	private int numDim = 0;
	public List<Batch> friendlyBatches;
	public List<Batch> unfriendlyBatches;
	
	public CubeLattice(String[] attributeNames){
		this.attributes = attributeNames;
		this.numDim = attributeNames.length;
	}
	
	public List<Cuboid> cuboids(){
		if (this.cuboids != null && this.cuboids.isEmpty())
			this.cuboids.clear();
		else
			this.cuboids = new ArrayList<Cuboid>();
		
		buildCuboids();
		return this.cuboids;
	}
	
	public void addCuboid(List<Integer> mask){
		Iterator<Integer> itor = mask.iterator();
		List<String> region = new ArrayList<String>();
		
		int index = 0;
		while(itor.hasNext()){
			if (itor.next() == 0)
				region.add(GlobalSettings.ALL);
			else
				region.add(this.attributes[index]);
			index++;
		}
		
		this.cuboids.add(new Cuboid(region.toArray(new String[region.size()])));
	}
	
	public void printCuboids(){
		BFS(cuboids.get(0));
	}
	
	public List<RollUp> toRollUps(CubeConverter converter, int pivot){
		return converter.toRollUps(this.attributes, pivot);
	}
	
	public List<RollUp> toRollUps(CubeConverter converter, List<Cuboid> regions, int pivot){
		return converter.toRollUps(this.attributes, regions, pivot);
	}
	
	private void buildCuboids(){
		int nRegion = (int) Math.pow(2, this.numDim);
		for(int i = 0; i < nRegion; i++){
			String binary = Integer.toBinaryString(i);
			
			int repeatTimes = this.numDim - binary.length();
			String temp = "";
			for(int j = 0; j < repeatTimes; j++)
				temp += "0";
			binary = temp + binary;
			
			List<String> cuboid = new ArrayList<String>();
			for(int j = 0; j < binary.length(); j++){
				if (binary.charAt(j) == '0'){
					cuboid.add(GlobalSettings.ALL);
				}else{
					cuboid.add(this.attributes[j]);
				}
			}
			
			Cuboid cb = new Cuboid(cuboid.toArray(new String[cuboid.size()]));
			cb.id = i;
			this.cuboids.add(cb);
		}
		
		for(int i = 0; i < this.cuboids.size(); i++){
			int value_of_parent = 0;
			
			Cuboid cuboid = cuboids.get(i);
			String[] attributes = cuboid.attributes;
			int length = attributes.length;
			
			List<Integer> index_of_zeros = new ArrayList<Integer>();
			
			
			for(int j = 0; j < length; j++){
				if (attributes[j].compareTo(GlobalSettings.ALL) != 0){
					value_of_parent += (int) Math.pow(2, length - j - 1);
				}else
					index_of_zeros.add(j);
			}
			
			cuboid.level = this.numDim - index_of_zeros.size();
			
			for(int j = 0; j < index_of_zeros.size(); j++){
				int index_of_child = value_of_parent + (int) Math.pow(2, length - index_of_zeros.get(j) - 1);
				Cuboid child = cuboids.get(index_of_child);
				cuboid.children.add(child);
				child.parents.add(cuboid);
			}
		}
	}
	
	public void printChildren(Cuboid... cubes){
		
		for(Cuboid cube: cubes){
			List<Cuboid> children = cube.getChildren();
			System.out.println(cube);
			for(int i = 0; i < children.size(); i++){
				Cuboid child = cube.children.get(i);
				System.out.println("\t" + child);
			}
			System.out.println("--------------------------------");
		}
	}
	
	public void printChildren(List<Cuboid> cubes){
		
		for(Cuboid cube: cubes){
			List<Cuboid> children = cube.getChildren();
			System.out.println(cube);
			for(int i = 0; i < children.size(); i++){
				Cuboid child = cube.children.get(i);
				System.out.println("\t" + child);
			}
			System.out.println("--------------------------------");
		}
	}

	public void batching(){
		friendlyBatches = new ArrayList<Batch>();
		unfriendlyBatches = new ArrayList<Batch>();
		
		Cuboid root = this.cuboids.get(0);
		
		if (root.isFriendly == false){
			Batch unfriendlyBatch = new Batch();
			root.isBatched = true;
			unfriendlyBatch.cuboids.add(root);
			unfriendlyBatch.isFriendly = false;
			unfriendlyBatch.partition_factor = root.partition_factor;
			unfriendlyBatches.add(unfriendlyBatch);
			
			// find initial batches
			initializingBatchArea(root);
			
		}else{
			Batch friendlyBatch = new Batch();
			friendlyBatch.isFriendly = true;
			friendlyBatch.cuboids.add(root);
			friendlyBatches.add(friendlyBatch);
			root.isBatched = true;
		}
		
		fillFriendlyBatchesBFS(root);
		
		for(Batch batch: friendlyBatches){
			batch.updateLowestAttributes();
		}
		
		for(Batch batch: unfriendlyBatches){
			batch.updateLowestAttributes();
		}
//		isolatingBatches(friendlyBatches);
//		isolatingBatches(unfriendlyBatches);
	}
	
	public void fillFriendlyBatchesBFS(Cuboid first){
		Queue<Cuboid> queue = new LinkedList<Cuboid>();
		queue.add(first);
		
		while(true){
			Cuboid root = queue.poll();
			if (root == null)
				return;
			
			//System.out.println("Considering " + root + "\n");
//			if (root.toString().equals("A,B,C,*,E,F"))
//				root = root;
			
			findAndJoinBatch(root);
			
			if (root.isBatched == false)
				queue.add(root);
			
			if (root.children.size() != 0){
				List<Cuboid> children = root.getChildren();
			
				for(int i = 0; i < children.size(); i++){
					Cuboid child = root.children.get(i);
					queue.add(child);
				}
			}	
		}
	}
	
	public void fillFriendlyBatchesDFS(Cuboid root){
		
		findAndJoinBatch(root);
		
		if (root.children.size() != 0){
			List<Cuboid> children = root.getChildren();
		
			for(int i = 0; i < children.size(); i++){
				fillFriendlyBatchesDFS(root.children.get(i));
			}
		}
	}
	
	public void DFS(Cuboid node){
		String indent = "";
		for(int i = 0; i < node.level; i++)
			indent += "\t";
		
		System.out.println(indent + node);
		
		List<Cuboid> children = node.getChildren();
		for(int i = 0; i < children.size(); i++)
			DFS(children.get(i));
	}
	
	public void BFS(Cuboid node){
		Map isTraversed = new HashMap<Cuboid, Boolean>();
		
		Queue<Cuboid> queue = new LinkedList<Cuboid>();
		queue.add(node);
		
		while(true){
			Cuboid root = queue.poll();
			if (root == null)
				return;
			
			if (isTraversed.get(root) == null){
				String indent = "";
				for(int i = 0; i < root.level; i++)
					indent += "\t";
				
				System.out.println(indent + root);
				isTraversed.put(root, true);
			}
			
			List<Cuboid> children = root.getChildren();
			
			for(int i = 0; i < children.size(); i++){
				Cuboid child = root.children.get(i);
				queue.add(child);
			}
		}
	}
	
	public void isolatingBatches(List<Batch> batches){
		for(int i = 0; i < batches.size(); i++){
			Batch batch = batches.get(i);
			List<Cuboid> cuboids = batch.cuboids;
			
			for(int j = 0; j < cuboids.size(); j++){
				Cuboid cuboid = cuboids.get(j);
				List<Cuboid> parents = cuboid.getParents();
				List<Cuboid> children = cuboid.getChildren();
				
				for(int t = 0; t < parents.size(); t++){
					if (cuboids.contains(parents.get(t)) == false)
						parents.remove(t);
				}
				
				for(int t = 0; t < children.size(); t++){
					if (cuboids.contains(children.get(t)) == false)
						children.remove(t);
				}
			}
		}
	}
	
	public void findAndJoinBatch(Cuboid root){
		if (root.isFriendly == true && root.isBatched == false){
			List<Cuboid> parents = root.getParents();
			List<Integer> index_of_possible_batch = new ArrayList<Integer>();
			
			for(int i = 0; i < parents.size(); i++){
				Cuboid parent = parents.get(i);
		
				for(int j = 0; j < friendlyBatches.size(); j++){
					List<Cuboid> cuboids = friendlyBatches.get(j).cuboids;
					if (cuboids.contains(parent)){
						index_of_possible_batch.add(j);
					}
						
				}
			}
			
			Collections.sort(index_of_possible_batch);
			
			for (int i = 0; i < index_of_possible_batch.size(); i++){
				List<Cuboid> cuboids = friendlyBatches.get(index_of_possible_batch.get(i)).cuboids;
				cuboids.add(root);
				/** TODO: check!!! **/
				if (checkSizeContraint() == true){
					root.isBatched = true;
					
					Collections.sort(friendlyBatches, new Comparator<Batch>(){
						@Override
						public int compare(Batch arg0,
								Batch arg1) {
							return arg0.cuboids.size() - arg1.cuboids.size();
						}});
					
					//printBatch(friendlyBatches.get(index_of_possible_batch.get(i)));
					//printBatches();
					break;
				}
				else
					cuboids.remove(cuboids.size() - 1);
				break;
			}
		}
	}
	
	
	
	public boolean checkSizeContraint(){
		for(int i = 0; i < friendlyBatches.size(); i++)
			for(int j = 0; j < friendlyBatches.size(); j++)
				if (Math.abs(friendlyBatches.get(i).cuboids.size() - friendlyBatches.get(j).cuboids.size()) > GlobalSettings. BATCH_SIZE_DIFF_THRES)
					return false;
		return true;
	}
	
	public void initializingBatchArea(Cuboid root){
		List<Cuboid> children = root.getChildren();
		
		for(int i = 0; i < children.size(); i++){
			Cuboid child = children.get(i);
			if (child.isFriendly == true && isAnyParentIsFriendly(child) == false && child.isBatched == false){
				Batch friendlyBatch = new Batch();
				friendlyBatch.isFriendly = true;
				friendlyBatch.cuboids.add(child);
				friendlyBatches.add(friendlyBatch);
				child.isBatched = true;
			}else if (child.isFriendly == false && child.isBatched == false){
				
				int index = -1;
				
				/** TODO: implement this corner case: in mr reducer **/
				
//				for (int j = 0; j < unfriendlyBatches.size(); j++){
//					if (unfriendlyBatches.get(j).partition_factor == child.partition_factor)
//						index = j;
//				}
				
				
				if (index == -1){
					Batch unfriendlyBatch = new Batch();
					unfriendlyBatch.isFriendly = false;
					unfriendlyBatch.cuboids.add(child);
					unfriendlyBatch.partition_factor = child.partition_factor;
					unfriendlyBatches.add(unfriendlyBatch);
				}else{
					unfriendlyBatches.get(index).cuboids.add(child);
				}
				
				child.isBatched = true;
				
				initializingBatchArea(child);
			}
		}
	}
	
	public void printBatches(){
		int sum = 0;
		for(int i = 0; i < friendlyBatches.size(); i++){
			Batch batch = friendlyBatches.get(i);
			sum += batch.cuboids.size();
			batch.print();
			
		}
		
		System.out.println("");
		
		for(int i = 0; i < unfriendlyBatches.size(); i++){
			Batch batch = unfriendlyBatches.get(i);
			sum += batch.cuboids.size();
			batch.print();	
		}
		
		System.out.println("Num of region: " + sum);
	}
	
	public boolean isAnyParentIsFriendly(Cuboid cuboid){
		List<Cuboid> parents = cuboid.getParents();
		for(int i = 0; i < parents.size(); i++){
			if (parents.get(i).isFriendly)
				return true;
		}
		return false;
	}
	
	/** stupid version **/
	private void computeCubeRegions(int mainOrigin, int origin, int dim, List<Integer> mask){
		addCuboid(mask);
		
		for(int i = dim; i < this.numDim; i++){
			if (mainOrigin == 0 && i == 0){
				mask.set(0, 1);
				addCuboid(mask);
			}
			
			for(int j = i + 1; j < this.numDim; j++){
				mask.set(j, 1);
				computeCubeRegions(-1, origin, j, mask);
				mask.set(j, 0);
			}
			
			if (mainOrigin == 0){
				origin = i + 1;
				if (origin < this.numDim){
					for(int k = 0; k < this.numDim; k++)
						mask.set(k, 0);
					mask.set(origin, 1);
					addCuboid(mask);
				}
			}
		}
	}
}
