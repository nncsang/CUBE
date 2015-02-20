package com.hadoop.cube.buc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;

import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.data_structure.Cuboid;
import com.hadoop.cube.data_writable.Tuple;
import com.hadoop.cube.utils.Utils;

public class BUC {
	public List<SortSegment> sortSegments;
	public List<Tuple> tuples;
	public List<Integer> partitionDim;
	public Tuple prevTuple;
	public SortSegment currentSortSegment;
	public static Tuple tempTuple;
	public static int[] nullArray;
	List<Cuboid> cuboids;
	
	public BUC(Batch batch){
		cuboids = new ArrayList<Cuboid>();
		cuboids.addAll(batch.cuboids);
		
		//sortSegments = BUC.findSortOrder(batch);
		findSortOrder();
		tuples = new ArrayList<Tuple>();
		partitionDim = batch.cuboids.get(0).numPresentation;
		prevTuple = null;
//		printSortSegments(sortSegments);
		nullArray = new int[Tuple.length];
		Arrays.fill(nullArray, -1);	
		
	}
	
	public BUC(String str){
		tuples = new ArrayList<Tuple>();
		prevTuple = null;
		nullArray = new int[Tuple.length];
		Arrays.fill(nullArray, -1);	
		
		String[] parts = str.split("b");
		String[] sortSegmentStrs = parts[0].split("a");
		
		sortSegments = new ArrayList<SortSegment>();
		for(String s: sortSegmentStrs){
			sortSegments.add(new SortSegment(s));
		}
		
		String[] nums = parts[1].split(";");
		partitionDim = new ArrayList<Integer>();
		for(String num: nums){
			partitionDim.add(Integer.parseInt(num));
		}
	}
	
	public String convertToString(){
		String str = "";
		for(int i = 0; i < sortSegments.size() - 1; i ++)
			str += sortSegments.get(i).convertToString() + "a";
		str += sortSegments.get(sortSegments.size() - 1).convertToString() + "b";
		str += Utils.joinI(partitionDim, ";");
		
		return str;
	}
	
	public void addTuple(Tuple tuple, LongWritable value){
		tuple.value = value;
		if (isNewPartition(tuple)){
			if (prevTuple != null){
//				for(int i = 0; i < sortSegments.size(); i++){
//					currentSortSegment = sortSegments.get(i);
//					outputPreviousGroup(tuples);
//				}
//				
				buc(tuples, cuboids, 0);
				tuples.clear();
			}
		}
		
		tuples.add(tuple);
		prevTuple = tuple;
	}
	
	public void finish(){
		buc(tuples, cuboids, 0);
		tuples.clear();
	}
	
	public boolean isNewPartition(Tuple tuple){
		
		if (Tuple.compareTo(tuple, prevTuple, partitionDim) !=0)
			return true;
		
		return false;
	}
	
	public void buc(List<Tuple> tuples, List<Cuboid> cuboids, int dim){
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
				if (cuboids.get(i).numPresentation.size() >= i + 1 && cuboids.get(i).numPresentation.get(dim) == sortOrder){
					newCuboids.add(cuboids.get(i));
					i++;
				}
				else
					break;
			}
			int end_shared_sort = i -  1;
			i = end_shared_sort;
			
			if (start_shared_sort == end_shared_sort){
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
							for(int t = start_shared_sort; t <= end_shared_sort; t++){
								buc(partition, newCuboids, dim + 1);
							}
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
	
	public void outputPreviousGroup(List<Tuple> tuples){
		
		/*Sort by sharedSort*/
		Collections.sort(tuples, new Comparator<Tuple>(){
			@Override
			public int compare(Tuple tuple1,
					Tuple tuple2) {
				return Tuple.compareTo(tuple1, tuple2, currentSortSegment.sharedSort);
			}});
		
		if (currentSortSegment.isNeedAggregateSharedSort == true){
			aggregateSortOrder(tuples, currentSortSegment.sharedSort);
		}
		
		BUC(tuples);
	}
	
	public void BUC(List<Tuple> tuples){
		for(final List<Integer> sortOder: currentSortSegment.sortOrder){
			Collections.sort(tuples, new Comparator<Tuple>(){
			@Override
			public int compare(Tuple tuple1, Tuple tuple2) {
					return Tuple.compareTo(tuple1, tuple2, sortOder);
				}});
				
			aggregateSortOrder(tuples, sortOder);
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
			
			sum = sum + tuple.value.get();
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
			
			sum = sum + tuple.value.get();
			prevTuple = tuple;
			
		}
		
		printOutput(prevTuple, sortOrder, sum);
	}
	
	public void printOutput(Tuple tuple, List<Integer> sortOrder, long sum){
		tempTuple = new Tuple(nullArray);
		
		for(Integer index: sortOrder){
			tempTuple.fields[index] = tuple.fields[index];
		}
		
		System.out.println(tempTuple + "\t" + sum);
	}
	
	private void findSortOrder(){
		List<List<Integer>> numPresentations = new ArrayList<List<Integer>>();
		
		int size = cuboids.size();
		
		for(int i = 0; i < size; i++)
			numPresentations.add(cuboids.get(i).numPresentation);
		
		for(int i = 0; i < size - 1; i++)
			for(int j = i + 1; j < size; j++){
				if (compareNumerically(numPresentations.get(i), numPresentations.get(j)) == 1){
					List<Integer> temp = numPresentations.get(i);
					numPresentations.set(i, numPresentations.get(j));
					numPresentations.set(j, temp);
				}
			}
		
		for(int i = 0; i < size; i++){
			System.out.println(Utils.joinI(numPresentations.get(i), ""));
		}
//		
//		int start_segment = 0;
//		int next_segment = 0;
//		
//		List<SortSegment> sortSegments = new ArrayList<SortSegment>();
//		
//		while(next_segment != -1){
//			
//			start_segment = next_segment;
//			next_segment = -1;
//			
//			SortSegment sortSegment = new SortSegment();
//			int longest_common_sub_length = numPresentations.get(start_segment).size();
//			for(int i = 0; i < longest_common_sub_length; i++){
//				boolean stop = false;
//				int dim = numPresentations.get(start_segment).get(i);
//				
//				for (int j = start_segment + 1; j < size; j++)
//					if (numPresentations.get(j).get(i) != dim){
//						stop = true;
//						if (i == 0)
//							next_segment = j;
//						break;
//					}
//				
//				if (stop)
//					break;
//				else
//					sortSegment.sharedSort.add(dim);
//			}
//			
//			if (sortSegment.sharedSort.size() == 0){
//				sortSegment.sharedSort = numPresentations.get(start_segment);
//			}
//			
//			longest_common_sub_length = sortSegment.sharedSort.size();
//			
//			if (longest_common_sub_length == numPresentations.get(start_segment).size()){
//				sortSegment.isNeedAggregateSharedSort = true;
//			}
//			
//			int end_segment = next_segment;
//			if (end_segment == -1)
//				end_segment = numPresentations.size();
//			
//			for(int i = start_segment; i < end_segment; i++){
//				List<Integer> sortOrder = new ArrayList<Integer>();
//				
//				List<Integer> presentation = numPresentations.get(i);
//				for(int j = longest_common_sub_length; j < presentation.size(); j++){
//					sortOrder.add(presentation.get(j));
//				}
//				
//				if (sortOrder.size() != 0)
//					sortSegment.sortOrder.add(sortOrder);
//			}
//			
//			sortSegments.add(sortSegment);
//		}
//		
//		return sortSegments;
	}
	
	public void printSortSegments(List<SortSegment> sortSegments){
		for(SortSegment ss: sortSegments){
			
			System.out.println("Need Aggregate Shared Sort: " + ss.isNeedAggregateSharedSort);
			System.out.println("Shared: " + Utils.joinI(ss.sharedSort, ""));
			
			for(List<Integer> sortOrder: ss.sortOrder){
				System.out.println("Sort order: " + Utils.joinI(sortOrder, ""));
			}
			System.out.println();
		}
		
		System.out.println("-------------------------------");
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
