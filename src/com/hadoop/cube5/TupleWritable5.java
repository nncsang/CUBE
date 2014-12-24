package com.hadoop.cube5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.cube.TimeStampWritable;

public class TupleWritable5 implements WritableComparable<TupleWritable5> {
    
	public int id;
	public TimeStampWritable timeStampWritable;
    public static int length = 5;
    
    public TupleWritable5() { 
    	this.id = -1;
    	this.timeStampWritable = new TimeStampWritable();
    }
    
    public TupleWritable5(int id, int[] f) {
    	this.id = id;
    	this.timeStampWritable = new TimeStampWritable(f);
    }
    
    public TupleWritable5(int id, int month, int dayOfMonth, int dayOfWeek, int flightNumber, int origin) {
    	this.id = id;
    	this.timeStampWritable = new TimeStampWritable(month, dayOfMonth, dayOfWeek, flightNumber, origin);
    }
    
    public void set(int id, int[] f) {
    	this.id = id;
        this.timeStampWritable.fields = f;
    }
    
    public void set(int id, int month, int dayOfMonth, int dayOfWeek, int flightNumber, int origin) {
    	this.id = id;
    	this.timeStampWritable.set(month, dayOfMonth, dayOfWeek, flightNumber, origin);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	this.id = in.readInt();
        this.timeStampWritable.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(this.id);
        this.timeStampWritable.write(out);
    }

    @Override
    public int compareTo(TupleWritable5 obj) {
    	if (obj == null)
    		return -1;
    	
    	if (obj.id == -1 || obj.timeStampWritable == null)
    		return -1;
    	
    	if (this.id < obj.id)
    		return -1;
    	
    	if (this.id > obj.id)
    		return 1;
    	
    	return this.timeStampWritable.compareTo(obj.timeStampWritable);
        
    }
    
    public String toString() {
        if (this.id == -1)
            return "";
        
        if (this.timeStampWritable == null)
        	return "";

        return this.id + ": \t" + this.timeStampWritable; 
    }
    
    /** A Comparator optimized for IntWritable. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(TupleWritable5.class);
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
          
          for (int i = 1; i <= TupleWritable5.length; i++) {
              i1 = readInt(b1, s1+ i*4);
              i2 = readInt(b2, s2+ i*4);
              
              if (i1 < i2) return -1;
              else if (i1 > i2) return 1;
          }
           
          return 0;
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(TupleWritable5.class, new Comparator());
    }
}