package com.hadoop.cube2rollups;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Utils {
	public static void printSet(Set<String> input){
		if (input == null || input.isEmpty())
			return;
		Iterator<String> itor = input.iterator();
		while(itor.hasNext()){
			System.out.println(itor.next());
		}
	}
	
	public static void headSet(Set<String> input, int num){
		if (input == null || input.isEmpty())
			return;
		Iterator<String> itor = input.iterator();
		int count = 0;
		while(itor.hasNext()){
			count++;
			System.out.println(itor.next());
			if (count == num)
				break;
		}
	}
	
	public static String joinB(boolean[] list, String delim) {
	    int len = list.length;
	    if (len == 0)
	        return "";
	    StringBuilder sb = new StringBuilder(Boolean.toString(list[0]));
	    for (int i = 1; i < len; i++) {
	        sb.append(delim);
	        sb.append(Boolean.toString(list[i]));
	    }
	    return sb.toString();
	}
	
	public static String join(List<String> list, String delim) {
	    int len = list.size();
	    if (len == 0)
	        return "";
	    StringBuilder sb = new StringBuilder(list.get(0).toString());
	    for (int i = 1; i < len; i++) {
	        sb.append(delim);
	        sb.append(list.get(i).toString());
	    }
	    return sb.toString();
	}
	
	public static String joinSetI(Set<Integer> list, String delim) {
	    int len = list.size();
	    if (len == 0)
	        return "";
	    Iterator<Integer> iter = list.iterator();
	    iter.hasNext();
	    StringBuilder sb = new StringBuilder(iter.next().toString());
	    while(iter.hasNext()) {
	        sb.append(delim);
	        sb.append(iter.next().toString());
	    }
	    return sb.toString();
	}
	
	public static String joinI(List<Integer> list, String delim) {
	    int len = list.size();
	    if (len == 0)
	        return "";
	    StringBuilder sb = new StringBuilder(list.get(0).toString());
	    for (int i = 1; i < len; i++) {
	        sb.append(delim);
	        sb.append(list.get(i).toString());
	    }
	    return sb.toString();
	}
	
	public static String join(String[] list, String delim) {
	    int len = list.length;
	    if (len == 0)
	        return "";
	    StringBuilder sb = new StringBuilder(list[0].toString());
	    for (int i = 1; i < len; i++) {
	        sb.append(delim);
	        sb.append(list[i].toString());
	    }
	    return sb.toString();
	}
}
