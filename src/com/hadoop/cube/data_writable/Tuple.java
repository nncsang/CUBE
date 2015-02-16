package com.hadoop.cube.data_writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Tuple implements WritableComparable<Tuple> {
    
    public static int length = 0;
    public static int NullValue = -1;
    public int[] fields = null;
    
    /*Called before creating any instances*/
    public static void setLength(int length){
    	Tuple.length = length;
    }
    
    public Tuple(){
    	this.fields = new int[Tuple.length];
    }
    
    public Tuple(int... f) {
    	this.fields = new int[Tuple.length];
    	if (f == null){
    		this.fields = f;
    		return;
    	}
    	
    	for(int i = 0; i < Tuple.length; i++)
    		this.fields[i] = f[i];
    }
    
    public void set(int... f) {
    	for(int i = 0; i < Tuple.length; i++)
    		this.fields[i] = f[i];
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < Tuple.length; i++)
            this.fields[i] = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < Tuple.length; i++)
            out.writeInt(this.fields[i]);
    }

    @Override
    public int compareTo(Tuple obj) {
    	if (obj == null)
    		return -1;
    	
    	if (obj.fields == null)
    		return -1;
    	
        for (int i = 0; i < Tuple.length; i++) {
            if (fields[i] < obj.fields[i])
                return -1;
            else if (fields[i] > obj.fields[i])
                return 1;
        }
        return 0;
        
    }
    
    public String toString() {
        if (fields == null)
            return "";
        String t = String.valueOf(fields[0]); 
        for (int i = 1; i < Tuple.length; i++) {
            t += "\t" + String.valueOf(fields[i]);
        }
        return t; 
    }
    
    /** A Comparator optimized for IntWritable. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(Tuple.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
          int i1, i2;
          for (int i = 0; i < Tuple.length; i++) {
              i1 = readInt(b1, s1+ i*4);
              i2 = readInt(b2, s2+ i*4);
              
              if (i1 < i2) return -1;
              else if (i1 > i2) return 1;
          }
           
          return 0;
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(Tuple.class, new Comparator());
    }
}