package com.hadoop.cube.irg_plus_irg;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.mapreduce.Partitioner;

public class HashPartitioner<K, V> extends Partitioner<K, V> {
    
    public MessageDigest m = null;
    protected int pivot = -1;
    
    public HashPartitioner() throws NoSuchAlgorithmException {
        super();
        m = MessageDigest.getInstance("MD5");
    }
    
    public int getPartition(K key, V value, int numReduceTasks) {      
        m.reset();
        m.update(key.toString().getBytes());
        return (m.digest()[15] & Integer.MAX_VALUE) % numReduceTasks;
    }
}