package com.hadoop.cube.data_structure;

import java.util.List;
import java.util.Set;

public interface CubeConverter {
	List<RollUp> toRollUps(String[] attributes, int pivot);
	List<RollUp> toRollUps(String[] attributes, Set<Region> regions, int pivot);
}
