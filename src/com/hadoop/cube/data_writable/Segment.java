package com.hadoop.cube.data_writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.cube.data_structure.Batch;
import com.hadoop.cube.utils.Utils;

public class Segment implements WritableComparable<Segment> {
    
	public int id;
	public Tuple tuple;
	
	public static List<Batch> batch;
	public static List<List<Integer>> partitionOrder = new ArrayList<List<Integer>>();
	public static List<List<Integer>> sortOrder = new ArrayList<List<Integer>>();
	public static List<Integer> partitionFactor = new ArrayList<Integer>();
	
	public static void updateSortOrder(){
		for(List<Integer> list: partitionOrder){
			List<Integer> order = new ArrayList<Integer>();
			order.addAll(list);
			
			for(int i = 0; i < Tuple.length; i++){
				if (order.contains(i) == false){
					order.add(i);
				}
			}
			
			//System.out.println("Sort order: " + Utils.joinI(order, ""));
			sortOrder.add(order);
		}
	}
	
	public Segment()
	{
		this.id = -1;
		this.tuple = new Tuple();
	}
	
    public Segment(int id, int... f) {
    	this.id = id;
    	this.tuple = new Tuple(f);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	this.id = in.readInt();
        this.tuple.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(this.id);
        this.tuple.write(out);
    }

    @Override
    public int compareTo(Segment obj) {
    	if (obj == null)
    		return -1;
    	
	    if (obj.tuple == null)
	    	return -1;
	    	
	    if (this.id < obj.id)
	    	return -1;
	    	
	    if (this.id > obj.id)
	    	return 1;
	    
	    if (sortOrder.size() == 0 || obj.id < 0)
	    	return this.tuple.compareTo(obj.tuple);
	    else
	    	return Tuple.compareTo(this.tuple, obj.tuple, sortOrder.get(id));
    }
    
    public String toString() {
        if (this.id == -1)
            return "";
        
        if (this.tuple == null)
        	return "";

        return this.id + ": \t" + this.tuple; 
    }
    
    /** A Comparator optimized for IntWritable. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(Segment.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
    	  int id1, id2;
    	  id1 = readInt(b1, s1);
    	  id2 = readInt(b2, s2);
    	  
  	      if (id1 < id2)
  	    	return -1;
  	    	
  	      if (id1 > id2)
  	    	return 1;
  	      
  	      if (sortOrder.size() == 0 || id1 < 0){
		      for (int i = 1; i <= Tuple.length; i++) {
		        	  
		         id1 = readInt(b1, s1 + i * 4);
		         id2 = readInt(b2, s2 + i * 4);
		              
		         if (id1 < id2) return -1;
		         	else if (id1 > id2) return 1;
		         }
  	      }else{
  	    	  List<Integer> sort = sortOrder.get(id1);
  	    	  for(Integer index: sort){
  	    		 id1 = readInt(b1, s1 + (index + 1) * 4);
		         id2 = readInt(b2, s2 + (index + 1) * 4);
		              
		         if (id1 < id2) return -1;
		         	else if (id1 > id2) return 1;
  	    	  }
  	      }
	     return 0;
    	 
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(Segment.class, new Comparator());
    }
}