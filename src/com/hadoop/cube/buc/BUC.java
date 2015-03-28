package com.hadoop.cube.buc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.utils.Utils;

public class BUC {
	public List<Tuple> tuples;
	public List<Integer> partitionDim;
	public Tuple prevTuple;
	public static Tuple tempTuple;
	public static int[] nullArray;
	public List<Cuboid> cuboids;
	public Context context;
	public LongWritable long_writable = new LongWritable(0);
	
	public void clear(){
		tuples.clear();
		partitionDim.clear();
		prevTuple = null;
		cuboids.clear();
		long_writable = null;
	}
	
	public void print(){
		for(Cuboid cuboid : cuboids){
			System.out.println(Utils.joinI(cuboid.numPresentation, ""));
		}
	}
	
	public BUC(Batch batch){
		cuboids = new ArrayList<Cuboid>();
		cuboids.addAll(batch.cuboids);
		findSortOrder();
		tuples = new ArrayList<Tuple>();
		partitionDim = batch.cuboids.get(0).numPresentation;
		prevTuple = null;
		nullArray = new int[Tuple.length];
		Arrays.fill(nullArray, -1);	
		context = null;
	}
	
	public BUC(String str, Context context){
		this.context = context;
		tuples = new ArrayList<Tuple>();
		prevTuple = null;
		nullArray = new int[Tuple.length];
		cuboids = new ArrayList<Cuboid>();
		
		Arrays.fill(nullArray, -1);	
		
		String[] parts = str.split("b");
		String[] cuboidStrs = parts[0].split(";");
		
		for(String s: cuboidStrs){
			cuboids.add(new Cuboid(s));
		}
		
		String[] nums = parts[1].split(";");
		partitionDim = new ArrayList<Integer>();
		for(String num: nums){
			partitionDim.add(Integer.parseInt(num));
		}
	}
	
	public String convertToString(){
		String str = "";
		
		for(int i = 0; i < cuboids.size() - 1; i++){
			str += cuboids.get(i).convertToString() + ";";
		}
		str += cuboids.get(cuboids.size() - 1).convertToString() + "b";
		str += Utils.joinI(partitionDim, ";");
		
		return str;
	}
	
	public void addTuple(Tuple tuple, long value){
		tuple.value = value;
		if (isNewPartition(tuple)){
			if (prevTuple != null){
				buc(tuples, cuboids, 0);
				tuples.clear();
			}else{
				prevTuple = new Tuple();
			}
		}
		
		tuples.add(tuple);
		prevTuple.fields = tuple.fields;
	}
	
	public void finish(){
		buc(tuples, cuboids, 0);
		clear();
	}
	
	public boolean isNewPartition(Tuple tuple){
		
		if (Tuple.compareTo(tuple, prevTuple, partitionDim) !=0)
			return true;
		
		return false;
	}
	
	public void buc(List<Tuple> tuples, List<Cuboid> cuboids, final int dim){
		for(int i = 0; i < cuboids.size(); i++){
			final List<Integer> numPresentation = cuboids.get(i).numPresentation;
			if (dim >= numPresentation.size())
				continue;
			
			final int sortOrder = numPresentation.get(dim);
			
			List<Cuboid> newCuboids = new ArrayList<Cuboid>();
			newCuboids.add(cuboids.get(i));
			
			int start_shared_sort = i;
			i++;
			while(i < cuboids.size()){
				if (cuboids.get(i).numPresentation.size() >= dim + 1 && cuboids.get(i).numPresentation.get(dim) == sortOrder){
					newCuboids.add(cuboids.get(i));
					i++;
				}
				else
					break;
			}
			int end_shared_sort = i -  1;
			i = end_shared_sort;
			
			
			
			if (start_shared_sort == end_shared_sort){
				Collections.sort(tuples, new Comparator<Tuple>(){
					@Override
					public int compare(Tuple tuple1,
							Tuple tuple2) {
						return Tuple.compareTo(tuple1, tuple2, numPresentation, dim);
					}});
				
				aggregateSortOrder(tuples, numPresentation, dim);
			}else{
				
				Collections.sort(tuples, new Comparator<Tuple>(){
					@Override
					public int compare(Tuple tuple1,
							Tuple tuple2) {
						return Tuple.compareTo(tuple1, tuple2, sortOrder);
					}});
				
				if (numPresentation.size() - 1 == dim){
					aggregateSortOrder(tuples, numPresentation, dim);
					newCuboids.remove(0);
				}
				
				Tuple tempTuple = new Tuple(nullArray);
				List<Tuple> partition = new ArrayList<Tuple>();
				
				for (int j = 0; j < tuples.size(); j++){
					if (Tuple.compareTo(tempTuple, tuples.get(j), numPresentation, 0, dim) != 0){
						if (partition.size() > 0){
							buc(partition, newCuboids, dim + 1);
							partition.clear();
						}
					}
					
					partition.add(tuples.get(j));
					
					tempTuple = tuples.get(j);
				}
				
				if (partition.size() > 0){
					buc(partition, newCuboids, dim + 1);
					partition.clear();
				}
			}
		}
	}
	
	
	public void aggregateSortOrder(List<Tuple> tuples, List<Integer> sortOrder, int start){
		long sum = 0;
		Tuple prevTuple = null;
		for(Tuple tuple: tuples){
			if (Tuple.compareTo(tuple, prevTuple, sortOrder, start) != 0){
				if (prevTuple != null){
					
					printOutput(prevTuple, sortOrder, sum);
				}
				sum = 0;
			}
			
			sum = sum + tuple.value;
			prevTuple = tuple;
			
		}
		
		printOutput(prevTuple, sortOrder, sum);
	}
	
	public void aggregateSortOrder(List<Tuple> tuples, List<Integer> sortOrder){
		long sum = 0;
		Tuple prevTuple = null;
		for(Tuple tuple: tuples){
			if (Tuple.compareTo(tuple, prevTuple, sortOrder) != 0){
				if (prevTuple != null){
					
					printOutput(prevTuple, sortOrder, sum);
				}
				sum = 0;
			}
			
			sum = sum + tuple.value;
			prevTuple = tuple;
			
		}
		
		printOutput(prevTuple, sortOrder, sum);
	}
	
	public void printOutput(Tuple tuple, List<Integer> sortOrder, long sum){
		tempTuple = new Tuple(nullArray);
		
		for(Integer index: sortOrder){
			tempTuple.fields[index] = tuple.fields[index];
		}
		
		if (context == null){
			//System.out.println(Utils.joinI(sortOrder, "") + ":\t\t" + tempTuple + "\t" + sum);
		}
		else{
			long_writable.set(sum);
			try{
				//System.out.println(Utils.joinI(sortOrder, "") + ":\t\t" + tempTuple + "\t" + sum);
				context.write(tempTuple, long_writable);
			}catch (Exception ex){
				
			}
		}
	}
	
	private void findSortOrder(){
		int size = cuboids.size();
		
		for(int i = 0; i < size - 1; i++)
			for(int j = i + 1; j < size; j++){
				if (compareNumerically(cuboids.get(i).numPresentation, cuboids.get(j).numPresentation) == 1){

					Cuboid tmpCuboid = cuboids.get(i);
					cuboids.set(i, cuboids.get(j));
					cuboids.set(j, tmpCuboid);
				}
			}
		
//		for(int i = 0; i < size; i++){
//			System.out.println(Utils.joinI(numPresentations.get(i), ""));
//		}
	}
	

	private static int compareNumerically(List<Integer> a, List<Integer> b){
		int size_a = a.size();
		int size_b = b.size();
		
		int size = Math.min(size_a, size_b);
		for(int i = 0; i < size; i++)
			if (a.get(i) < b.get(i))
				return -1;
			else if (a.get(i) > b.get(i))
				return 1;
		
		if (size_a < size_b)
			return -1;
		else if (size_a > size_b)
			return 1;
		
		return 0;
	}
}
