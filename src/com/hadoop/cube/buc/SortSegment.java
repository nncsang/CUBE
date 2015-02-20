package com.hadoop.cube.buc;

import java.util.ArrayList;
import java.util.List;

import com.hadoop.cube.utils.Utils;

public class SortSegment {
	public List<Integer> sharedSort;
	public List<List<Integer>> sortOrder;
	public boolean isNeedAggregateSharedSort;
	
	public SortSegment(){
		sharedSort = new ArrayList<Integer>();
		sortOrder = new ArrayList<List<Integer>>();
		isNeedAggregateSharedSort = false;
	}
	
	public SortSegment(String str){
		String[] parts = str.split("\n");
		if (parts[0].compareTo("0") == 0)
			isNeedAggregateSharedSort = false;
		else
			isNeedAggregateSharedSort = true;
		
		String[] sharedSorts = parts[1].split(";");
		sharedSort = new ArrayList<Integer>();
		
		for(String num: sharedSorts){
			sharedSort.add(Integer.parseInt(num));
		}
		
		sortOrder = new ArrayList<List<Integer>>();
		
		if (parts.length >= 3){
			String[] sortOrders = parts[2].split(",");
			for(String sort: sortOrders){
				List<Integer> order = new  ArrayList<Integer>();
				String[] nums = sort.split(";");
				for(String num: nums)
					order.add(Integer.parseInt(num));
				sortOrder.add(order);
			}
		}
	}
	
	public String convertToString(){
		String str = "";
		if (isNeedAggregateSharedSort == false)
			str += "0\n";
		else
			str += "1\n";
		str += Utils.joinI(sharedSort, ";");
		if (sortOrder.size() > 0){
			str += "\n";
			for(int i = 0; i < sortOrder.size() - 1; i++){
				str += Utils.joinI(sortOrder.get(i), ";") + ",";
			}
			
			str += Utils.joinI(sortOrder.get(sortOrder.size() - 1), ";");
		}
		return str;
	}
	
}
