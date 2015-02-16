package com.hadoop.cube.old_data_writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TupleWritable implements WritableComparable<TupleWritable> {
    
	public int id;
	public AirlineWritable airlineWritable;
    public static int length = 6;
    
    public TupleWritable() { 
    	this.id = -1;
    	this.airlineWritable = new AirlineWritable();
    }
    
    public TupleWritable(int id, int[] f) {
    	this.id = id;
    	this.airlineWritable = new AirlineWritable(f);
    }
    
    public TupleWritable(int id, int month, int dayOfMonth, int dayOfWeek, int flightNumber, int origin, int dest) {
    	this.id = id;
    	this.airlineWritable = new AirlineWritable(month, dayOfMonth, dayOfWeek, flightNumber, origin, dest);
    }
    
    public void set(int id, int[] f) {
    	this.id = id;
        this.airlineWritable.fields = f;
    }
    
    public void set(int id, int month, int dayOfMonth, int dayOfWeek, int flightNumber, int origin, int dest) {
    	this.id = id;
    	this.airlineWritable.set(month, dayOfMonth, dayOfWeek, flightNumber, origin, dest);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	this.id = in.readInt();
        this.airlineWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(this.id);
        this.airlineWritable.write(out);
    }

    @Override
    public int compareTo(TupleWritable obj) {
    	if (obj == null)
    		return -1;
    	
    	if (obj.id == -1 || obj.airlineWritable == null)
    		return -1;
    	
    	if (this.id < obj.id)
    		return -1;
    	
    	if (this.id > obj.id)
    		return 1;
    	
    	return this.airlineWritable.compareTo(obj.airlineWritable);
        
    }
    
    public String toString() {
        if (this.id == -1)
            return "";
        
        if (this.airlineWritable == null)
        	return "";

        return this.id + ": \t" + this.airlineWritable; 
    }
    
    /** A Comparator optimized for IntWritable. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(TupleWritable.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
          int i1, i2;
          
          int id1 = readInt(b1, s1);
          int id2 = readInt(b2, s2);
          
          if (id1 < id2)
        	  return -1;
          if (id1 > id2)
        	  return 1;
          
          for (int i = 1; i <= TupleWritable.length; i++) {
              i1 = readInt(b1, s1+ i*4);
              i2 = readInt(b2, s2+ i*4);
              
              if (i1 < i2) return -1;
              else if (i1 > i2) return 1;
          }
           
          return 0;
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(TupleWritable.class, new Comparator());
    }
}