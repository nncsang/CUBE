package com.hadoop.cube.data_writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Segment implements WritableComparable<Segment> {
    
	public int id;
	public Tuple tuple;
	
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
    	
    	if (obj.id == -1 || obj.tuple == null)
    		return -1;
    	
    	if (this.id < obj.id)
    		return -1;
    	
    	if (this.id > obj.id)
    		return 1;
    	
    	return this.tuple.compareTo(obj.tuple);
        
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
    	  int i1, i2;
          for (int i = 0; i <= Tuple.length; i++) {
        	  
              i1 = readInt(b1, s1 + i * 4);
              i2 = readInt(b2, s2 + i * 4);
              
              if (i1 < i2) return -1;
              else if (i1 > i2) return 1;
          }
           
          return 0;
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(Segment.class, new Comparator());
    }
}