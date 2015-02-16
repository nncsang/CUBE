package com.hadoop.cube.irg_plus_irg;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.hadoop.cube.settings.GlobalSettings;
import com.hadoop.cube.data_writable.Segment;
import com.hadoop.cube.data_writable.Tuple;

public class IRGPlusIRGPartitioner extends Partitioner<Segment, LongWritable> implements Configurable {
    
    protected MessageDigest m = null;
    protected int pivot = -1;
    protected int i;
    protected int choosen = 0;
    
    public IRGPlusIRGPartitioner() throws NoSuchAlgorithmException {
         m = MessageDigest.getInstance("MD5");
    }
    
    @Override
    public void setConf(Configuration conf) {
        pivot = Integer.parseInt(conf.get("hybrid.pivot", "-1"));
        choosen = Integer.parseInt(conf.get("choosen", "0"));
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public int getPartition(Segment segment, LongWritable value, int numReduceTasks) {
    	Tuple key = segment.tuple;
    	
        if (key.fields[pivot] == Tuple.NullValue) {
            return choosen % numReduceTasks;
        } else {
            m.reset();
            m.update(ByteBuffer.allocate(4).putInt(segment.id).array());
            
            for (i = 0; i <= pivot; i++) {
                m.update(ByteBuffer.allocate(4).putInt(key.fields[i]).array());
            }
            return (m.digest()[15] & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
}