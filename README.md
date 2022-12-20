
# Practically and Theoretically Efficient Garbage Collection for Multiversioning

This repository contains the artifact for the following paper:

> **Practically and Theoretically Efficient Garbage Collection for Multiversioning**<br />
> Yuanhao Wei, Guy E. Blelloch, Panagiota Fatourou, Eric Ruppert<br />
> In Proc. ACM Symposium on Principles and Practice of Parallel Programming (PPoPP), 2023.


## Getting Started
  - Software requirements: open-jdk11+, python3-matplotlib, any linux distribution
    - On Ubuntu 20.04, these dependencies can be installed with ```sudo apt-get install openjdk-17-jdk python3-matplotlib```
  - Hardware requirements: multi-core machine with 64GB main memory and ideally at least 64 cores
  - To compile and test the code, run:

```
    bash compile_all.sh    # compiles code
    bash run_all_tests.sh  # tests code
```

## Running experiments and generating graphs
  - To reproduce all the graphs in the paper, run ```bash generate_graphs_from_paper.sh```
      - Note: these steps assume ```bash compile_all.sh``` has already been executed
  - The output graphs will be stored in the graphs/ directory
  - Note that this command will take ~8 hours to run and requires a machine with 64G of RAM (information on how to tune JVM memory size can be found below). 
  - You can rerun a specific graph by running the corresponding command from the 
    generate_graphs_from_paper.sh file. Each command generates a single graph, except the ```_rqsize.py``` scripts which also generate a text file to store the table of version list lengths.
  - You can also run custom experiments (and generate graphs for them) using the following scripts: 
      - run_tree_experiments_scalability.py           : Tree scalability experiments (Figure 7)
      - run_tree_experiments_rqsize.py                : Tree range query size experiments (Figures 4-5)
      - run_hashtable_experiments_scalability.py      : Hashtable scalability experiments (Figure 8)
      - run_hashtable_experiments_rqsize.py           : Hashtable range query size experiments (Figures 6)

  - Use the "-h" option to see the parameters required by each script.
  - Usage examples:

```
      python3 run_java_experiments_scalability.py -h
      python3 run_java_experiments_scalability.py [num_keys] [ins-del-find-rq-rqsize] [thread_list] [outputfile] [num_repeats] [runtime] [JVM memory size] [zipf]
      python3 run_java_experiments_scalability.py  100000     25-25-49-1-1024         [1,4,16]       graph1.png   5             5         32G               0.99

      python3 run_tree_experiments_rqsize.py [num_keys] [update_threads] [rq_threads] [small_rq_threads] [rqsizelist] [outputfile] [num_repeats] [runtime] [JVM memory size] [zipf]
      python3 run_tree_experiments_rqsize.py 10000 4 4 4 [8,256] graph.png 5 5 32G 0
```
  - See generate_graphs_from_paper.sh for more examples of how to use the python scripts.
  - Parameter descriptions: 
      - JVM memory size: We set Java's heap size to 64GB in our experiments to avoid measuring GC time. 
        If your machine does not have 64GB of main memory, we recommend setting this value as high as possible.
      - num_repeats: In the Java benchmarks, each experiment is repeated (num_repeats x 2) times where the
        first half is used to warm up the JVM.
      - runtime: measured in seconds
      - thread_list: make sure there are no spaces in the lists. i.e. [1,2,3] instead of [1, 2, 3]. 
        Similarly for rqsizelist.
      - ins-del-find-rq-rqsize: The input 30-20-49-1-1024 indicates a workload with 30% inserts, 20% deletes
        49% finds, and 1% range queries of size 1024.
      - num_keys: number of keys to prefill the data structure with. As described in our experiments section,
        the key range is chosen so that the size of the data structure remains stable throughout the experiment.
      - rqsize: range query size
      - update_threads: number of update threads, each performing 50% inserts or 50% deletes
      - rq_threads: number of range query threads
      - rq_threads: number of small range query threads
      - query_threads: number of threads performing multi-point queries
      - threads: number of worker threads
      - zipf: Zipfian parameter, number between [0, 1), where 0 indicates uniform distribution

## Setting Parameters
  - Given a machine with ~128 logical cores, the graphs generated should be very similar to the ones reported in our paper
  - For machines with different numbers of cores, we recommend using the following settings to reproduce the general shape of our graphs:
    - Let X be 2 less than the number of logical cores on your machine
    - ```_rqsize.py``` experiments should be run with X/3 of each type of thread: update thread, rq thread, and small rq thread
    - ```_scalability.py``` experiments should be run with [1, X/2, X, 1.5\*X, 2\*X] threads

## Directory structure
  - The java code used for our experimental evaluation can be found in: ```java/src```
  - Code for the multiversion chromatic tree can be found in: ```java/src/algorithms/tree```
    - Contains 5 variations, each with a different MVGC scheme (SL-RT, DL-RT, BBF+, Steam+LF, and epoch based reclamation)
  - Code for the multiversion hashtable can be found in: ```java/src/algorithms/hashtable```
    - Same 5 variations as the multiversion tree
  - ```java/src/algorithms/common``` contains classes used by both the trees and the hashtables
  - Main file: ```java/src/main/Main.java```
