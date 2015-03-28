package com.hadoop.cube.settings;

import com.hadoop.cube.data_structure.CubeLattice;

public class GlobalSettings {
	public static String DELIM_BETWEEN_ATTRIBUTES = ",";
	public static String DELIM_BETWEEN_ROLLUPS = "-";
	public static String DELIM_BETWEEN_REGIONS = "|";
	public static String ALL = "*"; 
	public static String DELIM_BETWEEN_CONTENTS_OF_TUPLE = "\t";
	public static String DELIM_BETWEEN_GROUPIDS = ",";
	public static boolean ISDEBUG = false;
	public static int RANDOM_RATE = 10;
	public static CubeLattice cube;
	public static int BATCH_SIZE_DIFF_THRES = 3;
}
