package com.hadoop.cube.old_data_writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimeStampWritable implements WritableComparable<TimeStampWritable> {
    
    public static int length = 5;
    public static int NullValue = -1;
    public int[] fields = new int[length];
        
    public TimeStampWritable() {    
    }
    
    public TimeStampWritable(int[] f) {
        fields = f;
    }
    
    public TimeStampWritable(int year, int month, int dayOfMonth, int minute, int second) {
        fields[0] = year;
        fields[1] = month;
        fields[2] = dayOfMonth;
        fields[3] = minute;
        fields[4] = second;
    }
    
    public void set(int[] f) {
        fields = f;
    }
    
    public void set(int year, int month, int dayOfMonth, int minute, int second) {
        fields[0] = year;
        fields[1] = month;
        fields[2] = dayOfMonth;
        fields[3] = minute;
        fields[4] = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < length; i++)
            fields[i] = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < length; i++)
            out.writeInt(fields[i]);
    }

    @Override
    public int compareTo(TimeStampWritable obj) {
    	if (obj == null)
    		return -1;
    	
    	if (obj.fields == null)
    		return -1;
    	
        for (int i = 0; i < length; i++) {
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
        for (int i = 1; i < length; i++) {
            t += "\t" + String.valueOf(fields[i]);
        }
        return t; 
    }
    
    /** A Comparator optimized for IntWritable. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(TimeStampWritable.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
          int i1, i2;
          for (int i = 0; i < TimeStampWritable.length; i++) {
              i1 = readInt(b1, s1+ i*4);
              i2 = readInt(b2, s2+ i*4);
              
              if (i1 < i2) return -1;
              else if (i1 > i2) return 1;
          }
           
          return 0;
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(TimeStampWritable.class, new Comparator());
    }
}