
import sys 
import os

sys.path.insert(1, 'internals/')

import create_graphs as graph

if len(sys.argv) == 1 or sys.argv[1] == '-h':
  print("Usage: python3 run_tree_experiments_scalability.py [num_keys] [ins-del-find-rq-rqsize] [thread_list] [outputfile] [num_repeats] [runtime] [JVM memory size] [zipf]")
  print("For example: python3 run_tree_experiments_scalability.py 10000 3-2-95-0-0 [1,4] graph.png 1 1 1G 0")
  exit(0)

num_keys = sys.argv[1]
workload = sys.argv[2].split('-')
ins = workload[0]
rmv = workload[1]
print(ins + " " + rmv +'\n')
find = workload[2]
rq = workload[3]
rqsize = workload[4]
threads = sys.argv[3][1:-1].split(',')
rq_threads = '0'
small_rq_threads = '0'
# update_threads = sys.argv[2]
# rqsizes = sys.argv[5][1:-1].split(',')
graphfile = sys.argv[4]
repeats = int(sys.argv[5])*2
runtime = sys.argv[6]
JVM_mem_size = sys.argv[7]
batch_size = "1"
node_bind = "none"
zipf = float(sys.argv[8])
graphs_only = False
inc_update = ""
if "-g" in sys.argv:
  graphs_only = True
if "-inc-update" in sys.argv:
  inc_update = " -inc-update "

# th = int(update_threads)+int(rq_threads)+int(small_rq_threads)

key_range = int(int(num_keys) * (float(ins) + float(rmv)) / float(ins))

graphtitle = "scalability: " + num_keys + "keys-" + ins + "ins-" + rmv + "del-" + rq + "rq-" + rqsize + "rqsize" + inc_update
if zipf == 0:
  graphtitle += "-uniform"
else:
  graphtitle += "-" + str(zipf) + "zipf"

benchmark_name = graphtitle.replace(':','-').replace(' ', '-')
graphtitle = graphtitle.replace('-1leafsize','')
memory_results_file = "java/results/" + benchmark_name + ".memory.out"
throughput_results_file = "java/results/" + benchmark_name + ".throughput.csv"
# print(results_file_name)

datastructures = ["VcasChromaticBSTBBF", "VcasChromaticBSTSLRT", "VcasChromaticBSTSteamLF", "VcasChromaticBSTEpoch", "VcasChromaticBSTDLRT"]

# print(datastructures)

# "VcasChromaticBatchBSTSteamFast -param-64", "VcasChromaticBatchBSTSteamScan -param-64", 

# "ChromaticBatchBST -param-64", "BatchBST -param-64"

if node_bind == "":
  numactl = "numactl -i all "
elif node_bind == "none":
  numactl = ""
else:
  numactl = "numactl --membind=" + node_bind + " --cpunodebind=" + node_bind + " "

cmdbase = "java -server -Xss515m -Xms" + JVM_mem_size + " -Xmx" + "64G" + " -Xbootclasspath/a:'java/lib/scala-library.jar:java/lib/deuceAgent.jar' -jar java/build/experiments_instr.jar "

if not graphs_only:
  # delete previous results
  os.system("rm -rf java/build/*.csv")
  os.system("rm -rf java/build/*.csv_stdout")

  os.system("rm -rf java/build/*.out")

  i = 0
  for ds in datastructures:
    for th in threads:
      i = i+1
      tmp_results_file = "java/build/data-trials" + str(i) + ".out"
      zipf_param = ""
      if zipf != 0:
        zipf_param = " -zipf"
      cmd = "MAX_THREADS=258 " + numactl + cmdbase + str(th) + " " + str(repeats) + " " + str(runtime) + " " + ds + " -ins" + str(ins) + " -del" + str(rmv) + " -rq" + str(rq) + " -rqsize" + str(rqsize) + " -rqers" + rq_threads + " -smallrqers" + small_rq_threads + " -keys" + str(key_range) + zipf_param + " -prefill -memoryusage -file-java/build/data-trials" + str(i) + ".csv" + inc_update + " >> " + tmp_results_file
      f = open(tmp_results_file, "w")
      f.write(cmd + '\n')
      f.close()
      print(cmd)
      if os.system(cmd) != 0:
        print("error: script terminated early")
        exit(1)

  os.system("cat java/build/data-*.out > " + memory_results_file)
  os.system("cat java/build/data-*.csv > " + throughput_results_file)

graph.plot_java_scalability_memory_graphs(throughput_results_file, memory_results_file, graphfile, graphtitle)
