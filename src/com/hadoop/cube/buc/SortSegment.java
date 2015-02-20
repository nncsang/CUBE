package com.hadoop.cube.buc;

import java.util.ArrayList;
import java.util.List;

public class SortSegment {
	public List<Integer> sharedSort;
	public List<List<Integer>> sortOrder;
	public boolean isNeedAggregateSharedSort;
	
	public SortSegment(){
		sharedSort = new ArrayList<Integer>();
		sortOrder = new ArrayList<List<Integer>>();
		isNeedAggregateSharedSort = false;
	}
}
