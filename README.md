CUBE
====
In this project, I proposed a new algorithm for CUBE computation in distributed systems. Instead of dividing CUBE lattice into friendly and unfriendly patches like MRCUBE, the algorithm represents CUBE lattice by ROLLUPs. Then each ROLLUP is computed using IRG algorithm. This representation helps the algorithm overcome limits of MRCUBE which are not taking the advantage of sorting phase of most distributed systems (e.g. Hadoop, Spark...) and using a lot of memory in reducer phase where BUC is used. I used Map Reduce paradigm to implement and to demonstrate efficiency of the algorithm. Experiments are conducted with both synthetic and real data. Experiments show that the algorithm yields better performance and memory usage when compared with MRCUBE.

Related works
-----------------
- BUC:  **Bottom-Up Computation of Sparse and Iceberg CUBEs** of *Kevin Beyer and Raghu Ramakrishnan*. [For more information](https://github.com/nncsang/Hadoop-Cube-Bottom-Up-Computation)
- MRCUBE: **Distributed Cube Materialization on Holistic Measures** of Arnab Nandi, Cong Yu, Philip Bohannon, Raghu Ramakrishnan. [For more information](https://github.com/nncsang/Hadoop-Cube-MRCube) 
- IRG: **On the design space of MapReduce ROLLUP aggregates** of *DH Phan, M Dell'Amico, P Michiardi*

Experiments
-----------------
- Dataset: I used 6 datasets (5 dataset are synthetic, and the other is real data - ISH dataset)
- All the experiments are run under Hadoop cluster of Distributed Systems and Cloud Computing LAB at EURECOM
- Cluster information:  17 slave machines (8GB RAM and a 4-core CPU) with 2 map and 2 reduce slot each
- All results shown in the following are the average of 5 runs
- I used three main metrics for evaluation: runtime – i.e. job execution time – and total amount of work, i.e. the sum of individual task execution times, and phase runtime - i.e. map execution time, reducer execution time...
- Details of result can be viewed in [this document](/document/Experiment Result.pdf)

