import csv
import matplotlib as mpl
# mpl.use('Agg')
mpl.rcParams['grid.linestyle'] = ":"
mpl.rcParams.update({'font.size': 22})
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np
import os
import os.path
import statistics as st

columns = ['throughput', 'name', 'nthreads', 'ratio', 'maxkey', 'rqsize', 'time', 'ninstrue', 'ninsfalse', 'ndeltrue', 'ndelfalse', 'nrqtrue', 'nrqfalse', 'merged-experiment']

# print("running correct file")

# ["VcasIndirectBST", "VcasLibIndirectBST"] 

names = { 
          'VcasChromaticBSTEpoch': 'Tree-EBR',
          'VcasChromaticBSTSteamLF': 'Tree-Steam+LF',
          'VcasChromaticBSTBBF': 'Tree-BBF+', 
          'VcasChromaticBSTDLRT': 'Tree-DL-RT',
          'VcasChromaticBSTSLRT': 'Tree-SL-RT',

          'VcasHashtableEpoch': 'Hash-EBR',
          'VcasHashtableSteamLF': 'Hash-Steam+LF',
          'VcasHashtableDLRT': 'Hash-DL-RT',
          'VcasHashtableSLRT': 'Hash-SL-RT',
          'VcasHashtableBBF': 'Hash-BBF+',
          }
colors = { 

          'VcasChromaticBSTEpoch': 'C0',
          'VcasChromaticBSTSteamLF': 'C1',  
          'VcasChromaticBSTBBF': 'C6', 
          'VcasChromaticBSTDLRT': 'C2',
          'VcasChromaticBSTSLRT': 'C3',

          'VcasHashtableEpoch': 'C0',
          'VcasHashtableSteamLF': 'C1',
          'VcasHashtableDLRT': 'C2',
          'VcasHashtableSLRT': 'C3',
          'VcasHashtableBBF': 'C6',
}
linestyles = {
              'VcasChromaticBSTEpoch': '-',
              'VcasChromaticBSTSteamLF': '-', 
              'VcasChromaticBSTBBF': '-', 
              'VcasChromaticBSTDLRT': '-',
              'VcasChromaticBSTSLRT': '-',

              'VcasHashtableEpoch': '-',
              'VcasHashtableSteamLF': '-',
              'VcasHashtableDLRT': '-',
              'VcasHashtableSLRT': '-',
              'VcasHashtableBBF': '-',
}
markers =    { 
              'VcasChromaticBSTEpoch': 's',
              'VcasChromaticBSTSteamLF': 'D', 
              'VcasChromaticBSTBBF': '|', 
              'VcasChromaticBSTDLRT': 'o',
              'VcasChromaticBSTSLRT': '<',

              'VcasHashtableEpoch': 'o',
              'VcasHashtableSteamLF': 'x',
              'VcasHashtableDLRT': 's',
              'VcasHashtableSLRT': '>',
              'VcasHashtableBBF': '|',
}

# algList = ['KIWI', 'SnapTree', 'KSTRQ', 'BPBST64', 'LFCA', 'VcasBatchBSTGC64', 'VcasChromaticBatchBSTGC64']

# legendPrinted = False

def toStringBench(maxkey, ratio, rqsize):
  return str(maxkey) + 'k-' + ratio + '-' + str(rqsize) + 's'

def toRatio(insert, delete, rq):
  return str(insert) + 'i-' + str(delete) + 'd-' + str(rq) + 'rq'

def toString(algname, nthreads, ratio, maxkey, rqsize):
  return algname + '-' + str(nthreads) + 't-' + str(maxkey) + 'k-' + ratio + '-' + str(rqsize) + 's' 

def toString2(algname, nthreads, insert, delete, rq, maxkey, rqsize):
  return toString(algname, nthreads, toRatio(insert, delete, rq), maxkey, rqsize)

def toString3(algname, nthreads, benchmark):
  return algname + '-' + str(nthreads) + 't-' + benchmark

def toString4(algname, threadkey, ratio):
  return algname + '-' + threadkey + '-' + ratio

def toStringRQ(algname, nthreads, maxkey, rqsize):
  return "RQ-" + algname + '-' + str(nthreads) + 't-' + str(maxkey) + 'k-' + str(rqsize) + 's'

def toStringRQ3(algname, rqsize, benchmark, optype):
  return "RQ-" + algname + '-' + benchmark + '-' + str(rqsize) + 's' + '-' + optype

def toStringRQcomplex3(algname, benchmark, querytype, optype):
  if(querytype == 'range'):
    return toStringRQ3(algname, rqsize, benchmark, optype)
  return "RQ-" + algname + '-' + benchmark + '-' + querytype + '-' + optype

def div1Mlist(numlist):
  newList = []
  for num in numlist:
    newList.append(div1M(num))
  return newList

def div1M(num):
  return round(num/1000000.0, 3)

def avg(numlist):
  # if len(numlist) == 1:
  #   return numlist[0]
  total = 0.0
  length = 0
  for num in numlist:
    length=length+1
    total += float(num)
  if length > 0:
    return 1.0*total/length
  else:
    return -1;


def readJavaResultsFileMemory(filename, results, stddev, rqsizes, algs):
  resultsRaw = {}
  times = {}
  # results = {}
  # stddev = {}
  alg = ""
  rqsize = 0

  # read csv into resultsRaw
  file = open(filename, 'r')
  for line in file.readlines():
    if 'rqsize' in line:
      for word in line.split():
        if 'rqsize' in word:
          rqsize = int(word.replace('-rqsize', ''))
          if rqsize not in rqsizes:
            rqsizes.append(rqsize)
    if 'rqoverlapRandom' in line or 'rqZipf' in line:
      alg = line.split("-")[0]
      if alg not in algs:
        algs.append(alg)
    elif line.find('Memory Usage After Benchmark') != -1:
      key = alg + " " + str(rqsize)
      # print(key)
      if key not in resultsRaw:
        resultsRaw[key] = []
      resultsRaw[key].append(int(line.split(' ')[-2]))

  # print(resultsRaw)
  # Average througputRaw into results

  for key in resultsRaw:
    resultsAll = resultsRaw[key]
    warmupRuns = int(len(resultsAll)/2)

    resultsHalf = resultsAll[warmupRuns:]
    for i in range(len(resultsHalf)):
      resultsHalf[i] = resultsHalf[i]/1000000000.0
    avgResult = avg(resultsHalf)
    results[key] = avgResult
    stddev[key] = st.pstdev(resultsHalf)
    # print(avgResult)

def readJavaResultsFileJVMSize(filename, results, stddev, jvmsizes, algs):
  resultsRaw = {}
  times = {}
  # results = {}
  # stddev = {}
  alg = ""
  jvmsize = 0

  # read csv into resultsRaw
  file = open(filename, 'r')
  for line in file.readlines():
    if '-Xms' in line:
      for word in line.split():
        if '-Xms' in word:
          jvmsize = int(word.replace('-Xms', '').replace('G',''))
          if jvmsize not in jvmsizes:
            jvmsizes.append(jvmsize)
    if 'rqoverlapRandom' in line or 'rqZipf' in line:
      alg = line.split("-")[0]
      if alg not in algs:
        algs.append(alg)
    elif line.find('find+insert+delete throughput') != -1:
      key = alg + " " + str(jvmsize)
      if (key+'-updates') not in resultsRaw:
        resultsRaw[key+'-updates'] = []
      resultsRaw[key+'-updates'].append(float(line.split(' ')[-2]))
    elif line.find('RQ throughput:') != -1:
      key = alg + " " + str(jvmsize)
      if (key+'-rqs') not in resultsRaw:
        resultsRaw[key+'-rqs'] = []
      resultsRaw[key+'-rqs'].append(float(line.split(' ')[-2]))

  print(jvmsizes)
  print(algs)
  # Average througputRaw into results

  for key in resultsRaw:
    resultsAll = resultsRaw[key]
    warmupRuns = int(len(resultsAll)/2)
    if warmupRuns != 3:
      print(warmupRuns)
    resultsHalf = resultsAll[warmupRuns:]
    for i in range(len(resultsHalf)):
      resultsHalf[i] = resultsHalf[i]
    avgResult = avg(resultsHalf)
    results[key] = avgResult
    stddev[key] = st.pstdev(resultsHalf)
    # print(avgResult)
  print(results)
max_nodes_traversed_by_remove = 0

def readJavaResultsFileVersionListLength(filename, results, stddev, rqsizes, algs):
  global max_nodes_traversed_by_remove
  resultsRaw = {}
  times = {}
  # results = {}
  # stddev = {}
  alg = ""
  rqsize = 0

  if not os.path.exists(filename):
    return

  # read csv into resultsRaw
  file = open(filename, 'r')
  for line in file.readlines():
    if 'rqsize' in line:
      for word in line.split():
        if 'rqsize' in word:
          rqsize = int(word.replace('-rqsize', ''))
          if rqsize not in rqsizes:
            rqsizes.append(rqsize)
    if 'rqoverlapRandom' in line or 'rqZipf' in line:
      alg = line.split("-")[0]
      if alg not in algs:
        algs.append(alg)
    elif line.find('Average Version List Length (after)') != -1:
      key = alg + " " + str(rqsize)
      # print(key)
      if key not in resultsRaw:
        resultsRaw[key] = []
      resultsRaw[key].append(float(line.split(' ')[-1]))
    elif alg.find('LinearTime') != -1 and line.find('Average nodes traversed by remove') != -1:
      num = line.split(' ')[-1]
      # print('nodes traversed by remove ' + alg + ' : ' + num)
      if num != 'NaN':
        num = float(num)
        max_nodes_traversed_by_remove = max(num, max_nodes_traversed_by_remove)


  # print(resultsRaw)
  # Average througputRaw into results

  for key in resultsRaw:
    resultsAll = resultsRaw[key]
    warmupRuns = int(len(resultsAll)/2)
    # print(warmupRuns)
    resultsHalf = resultsAll[warmupRuns:]
    for i in range(len(resultsHalf)):
      resultsHalf[i] = resultsHalf[i]
    avgResult = avg(resultsHalf)
    results[key] = avgResult
    stddev[key] = st.pstdev(resultsHalf)
    # print(avgResult)

def readJavaResultsFileMemoryScalability(filename, results, stddev, threads, algs):
  resultsRaw = {}
  times = {}
  # results = {}
  # stddev = {}
  alg = ""
  th = 0

  # read csv into resultsRaw
  file = open(filename, 'r')
  for line in file.readlines():
    if 'thr-' in line:
      for word in line.split('-'):
        if 'thr' in word:
          th = int(word.replace('thr', ''))
          if th not in threads:
            threads.append(th)
    if 'rqoverlapRandom' in line or 'rqZipf' in line:
      alg = line.split("-")[0]
      if alg not in algs:
        algs.append(alg)
    elif line.find('Memory Usage After Benchmark') != -1:
      key = alg + " " + str(th)
      # print(key)
      if key not in resultsRaw:
        resultsRaw[key] = []
      resultsRaw[key].append(int(line.split(' ')[-2]))

  # print(resultsRaw)
  # Average througputRaw into results

  for key in resultsRaw:
    resultsAll = resultsRaw[key]
    warmupRuns = int(len(resultsAll)/2)
    # print(warmupRuns)
    resultsHalf = resultsAll[warmupRuns:]
    for i in range(len(resultsHalf)):
      resultsHalf[i] = resultsHalf[i]/1000000000.0
    avgResult = avg(resultsHalf)
    results[key] = avgResult
    stddev[key] = st.pstdev(resultsHalf)
    # print(avgResult)


def readJavaResultsFile(filename, throughput, stddev, threads, ratios, maxkeys, rqsizes, algs):
  columnIndex = {}
  throughputRaw = {}
  times = {}
  # throughput = {}
  # stddev = {}

  # read csv into throughputRaw
  with open(filename, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in csvreader:
      if not bool(columnIndex): # columnIndex dictionary is empty
        for col in columns:
          columnIndex[col] = row.index(col)
      if row[columnIndex[columns[0]]] == columns[0]:  # row contains column titles
        continue
      values = {}
      for col in columns:
        values[col] = row[columnIndex[col]]
      numUpdates = int(values['ninstrue']) + int(values['ninsfalse']) + int(values['ndeltrue']) + int(values['ndelfalse']) 
      numRQs = int(values['nrqtrue']) + int(values['nrqfalse'])
      if values['ratio'] == '50i-50d-0rq' and numRQs > 0:
        key = toStringRQ(values['name'], values['nthreads'], values['maxkey'], int(float(values['rqsize'])))
        if int(values['nthreads']) not in threads:
          threads.append(int(values['nthreads']))
        if int(values['maxkey']) not in maxkeys:
          maxkeys.append(int(values['maxkey']))
        if int(float(values['rqsize'])) not in rqsizes:
          rqsizes.append(int(float(values['rqsize'])))
        if values['name'] not in algs:
          algs.append(values['name'])
        time = float(values['time'])
        if values['merged-experiment'].find('findif') != -1:
          key += '-findif'
        elif values['merged-experiment'].find('succ') != -1:
          key += '-succ'
        elif values['merged-experiment'].find('multisearch-nonatomic') != -1:
          key += '-multisearch-nonatomic'
        elif values['merged-experiment'].find('multisearch') != -1:
          key += '-multisearch'
        if (key+'-updates') not in throughputRaw:
          throughputRaw[key+'-updates'] = []
          times[key+'-updates'] = []
        if (key+'-rqs') not in throughputRaw:
          throughputRaw[key+'-rqs'] = []
          times[key+'-rqs'] = []
        throughputRaw[key+'-updates'].append(numUpdates/time)
        throughputRaw[key+'-rqs'].append(numRQs/time)
        times[key+'-updates'].append(time)
        times[key+'-rqs'].append(time)
      else:
        key = toString(values['name'], values['nthreads'], values['ratio'], values['maxkey'], int(float(values['rqsize'])))
        if int(values['nthreads']) not in threads:
          threads.append(int(values['nthreads']))
        if int(values['maxkey']) not in maxkeys:
          maxkeys.append(int(values['maxkey']))
        if values['ratio'] not in ratios:
          ratios.append(values['ratio'])
        if int(float(values['rqsize'])) not in rqsizes:
          rqsizes.append(int(float(values['rqsize'])))
        if values['name'] not in algs:
          algs.append(values['name'])
        if key not in throughputRaw:
          throughputRaw[key] = []
          times[key] = []
        throughputRaw[key].append(float(values['throughput']))
        times[key].append(float(values['time']))

  for key in throughputRaw:
    resultsAll = throughputRaw[key]
    warmupRuns = int(len(resultsAll)/2)
    # print(warmupRuns)
    results = resultsAll[warmupRuns:]
    for i in range(len(results)):
      results[i] = results[i]/1000000.0
    if avg(results) == 0:
      continue
    avgResult = avg(results)
    throughput[key] = avgResult
    stddev[key] = st.pstdev(results)
    # print(avgResult)

  # return throughput, stddev

def plot_rqsize_memory_graphs(inputFile, outputFile, graphtitle, throughput, throughput_stddev, memory, memory_stddev, threads, ratios, maxkeys, rqsizes, algs):
  threads.sort()
  rqsizes.sort()
  maxkeys.sort()

  bench = str(threads[0]) + 't-' + str(maxkeys[0]) + 'k'

  ax = {}
  fig, (ax['rqs'], ax['updates'], ax['memory']) = plt.subplots(1, 3, figsize=(19,7))

  for opType in ('rqs', 'updates'):
    series = {}
    error = {}
    ymax = 0
    for alg in algs:
      if 'Baseline' in alg:
        continue
      # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
      #   continue
      series[alg] = []
      error[alg] = []
      for size in (rqsizes[0:] if opType == 'rqs' else rqsizes):
        if toStringRQ3(alg, size, bench, opType) not in throughput:
          del series[alg]
          del error[alg]
          break
        series[alg].append(throughput[toStringRQ3(alg, size, bench, opType)]*(size if opType == 'rqs' else 1))
        error[alg].append(throughput_stddev[toStringRQ3(alg, size, bench, opType)]/(size if opType == 'rqs' else 1))
    # create plot
    
    opacity = 0.8
    rects = {}
     
    for alg in series:
      if max(series[alg]) > ymax:
        ymax = max(series[alg])
      # print(alg)
      rects[alg] = ax[opType].errorbar((rqsizes[0:] if opType == 'rqs' else rqsizes), series[alg], 
        error[alg], capsize=3,
        alpha=opacity,
        color=colors[alg],
        #hatch=hatch[ds],
        linestyle=linestyles[alg],
        marker=markers[alg],
        markersize=8,
        label=names[alg])
    ax[opType].set_xscale('log', basex=2)
    # rqlabels=(8, 64, 258, '1K', '8K', '64K')
    # plt.xticks(rqsizes, rqlabels)
    # if opType == 'rqs':
    #   ax[opType].set_yscale('log')
    # else:
    ax[opType].set_ylim(bottom=-0.02*ymax)
    if opType == 'rqs' and 'table' in graphtitle:
      ax[opType].set(xlabel='Size of large rtxs', ylabel='Read throughput (reads / $\mu$s)')
    elif opType == 'rqs':
      ax[opType].set(xlabel='Size of large rtxs', ylabel='Read throughput (nodes / $\mu$s)')
    else:
      ax[opType].set(xlabel='Size of large rtxs', ylabel='Update throughput (ops / $\mu$s)')
    ax[opType].grid()
    ax[opType].title.set_text(opType.replace('rqs', 'Read transaction').replace('updates', 'Update') + ' throughput')

  # plot memory graphs
  series = {}
  error = {}
  ymax = 0
  ymax2 = 0
  for alg in algs:
    # if 'Epoch' in alg:
    #     continue
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
    #   continue
    series[alg] = []
    error[alg] = []
    for size in rqsizes:
      key = alg + " " + str(size)
      if key not in memory:
        del series[alg]
        del error[alg]
        break
      series[alg].append(memory[key])
      error[alg].append(memory_stddev[key])
  # create plot
  
  opacity = 0.8
  rects = {}
   
  for alg in series:
    if max(series[alg]) > ymax:
      ymax2 = ymax;
      ymax = max(series[alg])
    elif max(series[alg]) > ymax2:
      ymax2 = max(series[alg])
    # if 'Epoch' not in alg and max(series[alg]) > ymax:
    #   ymax = max(series[alg])
    # print(alg)
    rects[alg] = ax['memory'].errorbar(rqsizes, series[alg], 
      error[alg], capsize=3,
      alpha=opacity,
      color=colors[alg],
      #hatch=hatch[ds],
      linestyle=linestyles[alg],
      marker=markers[alg],
      markersize=8,
      label="CT-64 (non-atmoic RQs)" if names[alg] == "CT-64" else names[alg])
  ax['memory'].set_xscale('log', basex=2)
  # rqlabels=(8, 64, 258, '1K', '8K', '64K')
  # plt.xticks(rqsizes, rqlabels)
  # print("Ymax: " + str(ymax))
  # print("Ymax2: " + str(ymax2))
  # if ymax2 != 0 and ymax > 5*ymax2 and 'Hashtable' in alg:
  #   ymax = ymax2

  # ax['memory'].set_ylim(bottom=-0.02*ymax,top=1.05*ymax)
  ax['memory'].set(xlabel='Size of large rtxs', ylabel='Memory Usage (GB)')
  ax['memory'].grid()
  ax['memory'].title.set_text("Memory usage")

  legend_x = 1
  legend_y = 0.5 
  fig.suptitle(graphtitle.replace("000000k", "Mk").replace("000k", "Kk").replace("keys-", " keys with ").replace("up-", " update threads and ").replace("rq", " range query threads"), fontsize=25)
  fig.tight_layout()
  fig.subplots_adjust(top=0.78)
  plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  plt.savefig(outputFile, bbox_inches='tight')
  plt.close('all')

def plot_space_time_graphs(inputFile, outputFile, graphtitle, throughput, throughput_stddev, rqsize, jvmsizes, algs):
  ax = {}
  fig, (ax['rqs'], ax['updates']) = plt.subplots(1, 2, figsize=(19,7))

  for opType in ('rqs', 'updates'):
    series = {}
    error = {}
    ymax = 0
    for alg in algs:
      if 'Baseline' in alg:
        continue
      # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
      #   continue
      series[alg] = []
      error[alg] = []
      for size in jvmsizes:
        key = alg + " " + str(size) + "-" + opType
        if key not in throughput:
          del series[alg]
          del error[alg]
          break
        series[alg].append(throughput[key]*(rqsize if opType == 'rqs' else 1))
        error[alg].append(throughput_stddev[key]/(rqsize if opType == 'rqs' else 1))
    # create plot
    
    opacity = 0.8
    rects = {}
     
    for alg in series:
      if max(series[alg]) > ymax:
        ymax = max(series[alg])
      # print(alg)
      rects[alg] = ax[opType].errorbar(jvmsizes, series[alg], 
        error[alg], capsize=3,
        alpha=opacity,
        color=colors[alg],
        #hatch=hatch[ds],
        linestyle=linestyles[alg],
        marker=markers[alg],
        markersize=8,
        label=names[alg])
    ax[opType].set_xscale('log', basex=2)
    # rqlabels=(8, 64, 258, '1K', '8K', '64K')
    # plt.xticks(rqsizes, rqlabels)
    # if opType == 'rqs':
    #   ax[opType].set_yscale('log')
    # else:
    ax[opType].set_ylim(bottom=-0.02*ymax)
    if opType == 'rqs' and 'table' in graphtitle:
      ax[opType].set(xlabel='JVM heap size', ylabel='Read throughput (reads / $\mu$s)')
    elif opType == 'rqs':
      ax[opType].set(xlabel='JVM heap size', ylabel='Read throughput (nodes / $\mu$s)')
    else:
      ax[opType].set(xlabel='JVM heap size', ylabel='Update throughput (ops / $\mu$s)')
    ax[opType].grid()
    ax[opType].title.set_text(opType.replace('rqs', 'Read transaction').replace('updates', 'Update') + ' throughput')

  legend_x = 1
  legend_y = 0.5 
  fig.suptitle(graphtitle.replace("000000k", "Mk").replace("000k", "Kk").replace("keys-", " keys with ").replace("up-", " update threads and ").replace("rq", " range query threads"), fontsize=25)
  fig.tight_layout()
  fig.subplots_adjust(top=0.78)
  plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  plt.savefig(outputFile, bbox_inches='tight')
  plt.close('all')

def export_legend(legend, filename="legend.png"):
    fig  = legend.figure
    fig.canvas.draw()
    bbox  = legend.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(filename, dpi="figure", bbox_inches=bbox)

def print_legend(inputFile, outputFile, graphtitle, throughput, throughput_stddev, memory, memory_stddev, threads, ratios, maxkeys, rqsizes, algs):
  threads.sort()
  rqsizes.sort()
  maxkeys.sort()

  bench = str(threads[0]) + 't-' + str(maxkeys[0]) + 'k'

  ax = {}
  fig, (ax['rqs'], ax['updates'], ax['memory']) = plt.subplots(1, 3, figsize=(19,7))

  for opType in ('rqs', 'updates'):
    series = {}
    error = {}
    ymax = 0
    for alg in algs:
      if 'Baseline' in alg:
        continue
      # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
      #   continue
      series[alg] = []
      error[alg] = []
      for size in (rqsizes[0:] if opType == 'rqs' else rqsizes):
        if toStringRQ3(alg, size, bench, opType) not in throughput:
          del series[alg]
          del error[alg]
          break
        series[alg].append(throughput[toStringRQ3(alg, size, bench, opType)]*(size if opType == 'rqs' else 1))
        error[alg].append(throughput_stddev[toStringRQ3(alg, size, bench, opType)]/(size if opType == 'rqs' else 1))
    # create plot
    
    opacity = 0.8
    rects = {}
     
    for alg in series:
      if max(series[alg]) > ymax:
        ymax = max(series[alg])
      # print(alg)
      rects[alg] = ax[opType].plot((rqsizes[0:] if opType == 'rqs' else rqsizes), series[alg], 
        # capsize=3,
        alpha=opacity,
        color=colors[alg],
        #hatch=hatch[ds],
        linestyle=linestyles[alg],
        marker=markers[alg],
        markersize=8,
        label=names[alg].replace('Tree-','').replace('Hash-',''))
    ax[opType].set_xscale('log', basex=2)
    # rqlabels=(8, 64, 258, '1K', '8K', '64K')
    # plt.xticks(rqsizes, rqlabels)
    # if opType == 'rqs':
    #   ax[opType].set_yscale('log')
    # else:
    ax[opType].set_ylim(bottom=-0.02*ymax)
    if opType == 'rqs' and 'table' in graphtitle:
      ax[opType].set(xlabel='Size of large rtxs', ylabel='Read throughput (reads / $\mu$s)')
    elif opType == 'rqs':
      ax[opType].set(xlabel='Size of large rtxs', ylabel='Read throughput (nodes / $\mu$s)')
    else:
      ax[opType].set(xlabel='Size of large rtxs', ylabel='Update throughput (ops / $\mu$s)')
    ax[opType].grid()
    ax[opType].title.set_text(opType.replace('rqs', 'Read transaction').replace('updates', 'Update') + ' throughput')

  # plot memory graphs
  series = {}
  error = {}
  ymax = 0
  ymax2 = 0
  for alg in algs:
    # if 'Epoch' in alg:
    #     continue
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
    #   continue
    series[alg] = []
    error[alg] = []
    for size in rqsizes:
      key = alg + " " + str(size)
      if key not in memory:
        del series[alg]
        del error[alg]
        break
      series[alg].append(memory[key])
      error[alg].append(memory_stddev[key])
  # create plot
  
  opacity = 0.8
  rects = {}
   
  for alg in series:
    if max(series[alg]) > ymax:
      ymax2 = ymax;
      ymax = max(series[alg])
    elif max(series[alg]) > ymax2:
      ymax2 = max(series[alg])
    # if 'Epoch' not in alg and max(series[alg]) > ymax:
    #   ymax = max(series[alg])
    # print(alg)
    rects[alg] = ax['memory'].plot(rqsizes, series[alg], 
      # capsize=3,
      alpha=opacity,
      color=colors[alg],
      #hatch=hatch[ds],
      linestyle=linestyles[alg],
      marker=markers[alg],
      markersize=8,
      label="CT-64 (non-atmoic RQs)" if names[alg] == "CT-64" else names[alg].replace('Tree-','').replace('Hash-',''))
  ax['memory'].set_xscale('log', basex=2)
  # rqlabels=(8, 64, 258, '1K', '8K', '64K')
  # plt.xticks(rqsizes, rqlabels)
  # print("Ymax: " + str(ymax))
  # print("Ymax2: " + str(ymax2))
  # if ymax2 != 0 and ymax > 5*ymax2 and 'Hashtable' in alg:
  #   ymax = ymax2

  ax['memory'].set_ylim(bottom=-0.02*ymax,top=1.05*ymax)
  ax['memory'].set(xlabel='Size of large rtxs', ylabel='Memory Usage (GB)')
  ax['memory'].grid()
  ax['memory'].title.set_text("Memory usage")

  legend_x = 1
  legend_y = 0.5 
  fig.suptitle(graphtitle.replace("000000k", "Mk").replace("000k", "Kk").replace("keys-", " keys with ").replace("up-", " update threads and ").replace("rq", " range query threads"), fontsize=25)
  fig.tight_layout()
  fig.subplots_adjust(top=0.78)

  legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=7, framealpha=0.0)
  export_legend(legend, 'graphs/' + 'legend.png')
  plt.close('all')


def gen_table_rqsize(result, outputFile, stddev, threads, ratios, maxkeys, rqsizes, algs):
  threads.sort()
  rqsizes.sort()
  maxkeys.sort()

  bench = str(threads[0]) + 't-' + str(maxkeys[0]) + 'k'

  series = {}
  error = {}
  line = "RQ size: "
  for size in rqsizes:
    line += str(size) + " "
  f = open(outputFile, "w")
  f.write(line+'\n')
  for alg in algs:
    # if 'Epoch' in alg:
    #     continue
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
    #   continue
    series[alg] = []
    error[alg] = []
    line = names[alg] + ': '
    for size in rqsizes:
      key = alg + " " + str(size)
      if key not in result:
        del series[alg]
        del error[alg]
        break
      line += str(round(result[key], 2)) + " "
      series[alg].append(result[key])
      error[alg].append(stddev[key])
    f.write(line+'\n')
  f.close()
  


def plot_scalability_memory_graphs(inputFile, outputFile, graphtitle, throughput, throughput_stddev, memory, memory_stddev, threads, ratios, maxkeys, rqsizes, algs):
  threads.sort()
  rqsizes.sort()
  maxkeys.sort()

  bench = toStringBench(maxkeys[0], ratios[0], rqsizes[0])
  # bench = str(threads[0]) + 't-' + str(maxkeys[0]) + 'k'

  ax = {}
  fig, (ax['throughput'], ax['memory']) = plt.subplots(1, 2, figsize=(14,7))
  # fig, (ax['throughput']) = plt.subplots(1, 1, figsize=(23,7))

  series = {}
  error = {}
  ymax = 0
  for alg in algs:
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
    #   continue
    # print(alg)
    if "Baseline" in alg:
      continue
    series[alg] = []
    error[alg] = []
    for th in threads:
      if toString3(alg, th, bench) not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[toString3(alg, th, bench)])
      error[alg].append(throughput_stddev[toString3(alg, th, bench)])
    # create plot
    
    opacity = 0.8
    rects = {}
     
  for alg in series:
    if max(series[alg]) > ymax:
      ymax = max(series[alg])
    # print(alg)
    rects[alg] = ax['throughput'].errorbar(threads, series[alg], 
      error[alg], capsize=3,
      alpha=opacity,
      color=colors[alg],
      #hatch=hatch[ds],
      linestyle=linestyles[alg],
      marker=markers[alg],
      markersize=8,
      label=names[alg])
  # ax['throughput'].set_xscale('log', basex=2)
  # rqlabels=(8, 64, 258, '1K', '8K', '64K')
  # plt.xticks(rqsizes, rqlabels)
  # ax['throughput'].axvline(x=128, color='k', linestyle='--')
  ax['throughput'].set_ylim(bottom=-0.02*ymax)
  ax['throughput'].set(xlabel='Number of Threads', ylabel='Total throughput (ops / $\mu$s)')
  ax['throughput'].grid()
  ax['throughput'].title.set_text('Throughput')
    # ax['throughput'].title.set_text(opType.replace('rqs', 'Read transaction').replace('updates', 'Update') + ' throughput')

  # plot memory graphs
  series = {}
  error = {}
  ymax = 0
  for alg in algs:
    # if (alg == 'BatchBST64' or alg == 'ChromaticBatchBST64') and bench.find('-0rq') == -1:
    #   continue
    series[alg] = []
    error[alg] = []
    for th in threads:
      key = str(alg) + " " + str(th)
      if key not in memory:
        del series[alg]
        del error[alg]
        break
      series[alg].append(memory[key])
      error[alg].append(memory_stddev[key])
  # create plot
  
  opacity = 0.8
  rects = {}
   
  for alg in series:
    if max(series[alg]) > ymax:
      ymax = max(series[alg])
    # print(alg)
    rects[alg] = ax['memory'].errorbar(threads, series[alg], 
      error[alg], capsize=3,
      alpha=opacity,
      color=colors[alg],
      #hatch=hatch[ds],
      linestyle=linestyles[alg],
      marker=markers[alg],
      markersize=8,
      label="CT-64 (non-atmoic RQs)" if names[alg] == "CT-64" else names[alg])
  # ax['memory'].set_xscale('log', basex=2)
  # rqlabels=(8, 64, 258, '1K', '8K', '64K')
  # plt.xticks(rqsizes, rqlabels)
  # ax['memory'].axvline(x=128, color='k', linestyle='--')
  ax['memory'].set_ylim(bottom=-0.02*ymax)
  ax['memory'].set(xlabel='Number of Threads', ylabel='Memory Usage (GB)')
  ax['memory'].grid()
  ax['memory'].title.set_text("Memory usage")

  legend_x = 1.1
  legend_y = 0.5 
  fig.suptitle(graphtitle.replace("000000k", "Mk").replace("000k", "Kk").replace("keys-", " keys with ").replace("up-", " update threads and "), fontsize=25)
  fig.tight_layout()
  fig.subplots_adjust(top=0.78)
  plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y))
  plt.savefig(outputFile, bbox_inches='tight')
  plt.close('all')


algs_order = ['VcasChromaticBSTSLRT', 'VcasChromaticBSTDLRT', 'VcasChromaticBSTBBF', 'VcasChromaticBSTSteamLF', 'VcasChromaticBSTEpoch', 'VcasHashtableSLRT', 'VcasHashtableDLRT', 'VcasHashtableBBF', 'VcasHashtableSteamLF', 'VcasHashtableEpoch']

def plot_java_rqsize_memory_graphs(inputFileThroughput, inputFileMemory, outputFile, graphtitle):
  throughput = {}
  throughput_stddev = {}
  memory = {}
  memory_stddev = {}
  versionListLength = {}
  version_stddev = {}
  threads = []
  ratios = []
  maxkeys = []
  rqsizes = []
  algs = []

  readJavaResultsFile(inputFileThroughput, throughput, throughput_stddev, threads, ratios, maxkeys, rqsizes, algs)
  readJavaResultsFileMemory(inputFileMemory, memory, memory_stddev, rqsizes, algs)
  readJavaResultsFileVersionListLength(inputFileMemory, versionListLength, version_stddev, rqsizes, algs)

  # print(algs)

  rqsizes.sort()
  algs = [x for x in algs if 'Baseline' not in x]

  algs_tmp = []
  for alg in algs_order:
    if alg in algs:
      algs_tmp.append(alg)
  algs = algs_tmp

  print(graphtitle)
  # print(algs)
  # print(throughput)
  # print(memory)

  plot_rqsize_memory_graphs(inputFileThroughput, outputFile, graphtitle, throughput, throughput_stddev, memory, memory_stddev, threads, ratios, maxkeys, rqsizes, algs)

  # print_legend(inputFileThroughput, outputFile, graphtitle, throughput, throughput_stddev, memory, memory_stddev, threads, ratios, maxkeys, rqsizes, algs)

  if len(versionListLength) > 0:
    gen_table_rqsize(versionListLength, outputFile.replace('.png', '')+'.txt', version_stddev, threads, ratios, maxkeys, rqsizes, algs)

  print('max nodes traversed by remove: ' + str(max_nodes_traversed_by_remove))

# def compare_mem_sizes(mem_size1, mem_size2):
#   size1 = int(mem_size1[:-1])
#   size2 = int(mem_size2[:-1])
#   if fitness(size1) < fitness(size2):
#       return -1
#   elif fitness(size1) > fitness(size2):
#       return 1
#   else:
#       return 0

def plot_jvm_size_graphs(inputFileThroughput, inputFileMemory, outputFile, graphtitle):
  throughput = {}
  throughput_stddev = {}
  jvmsizes = []
  algs = []

  readJavaResultsFileJVMSize(inputFileMemory, throughput, throughput_stddev, jvmsizes, algs)

  # print(algs)

  # jvmsizes.sort(key=compare_mem_sizes)
  jvmsizes.sort()

  algs = [x for x in algs if 'Baseline' not in x]

  algs_tmp = []
  for alg in algs_order:
    if alg in algs:
      algs_tmp.append(alg)
  algs = algs_tmp

  print(graphtitle)
  # print(algs)
  # print(throughput)
  # print(memory)

  plot_space_time_graphs(inputFileThroughput, outputFile, graphtitle, throughput, throughput_stddev, 20000000, jvmsizes, algs)

def plot_java_scalability_memory_graphs(inputFileThroughput, inputFileMemory, outputFile, graphtitle):
  throughput = {}
  throughput_stddev = {}
  memory = {}
  memory_stddev = {}
  threads = []
  ratios = []
  maxkeys = []
  rqsizes = []
  algs = []

  readJavaResultsFile(inputFileThroughput, throughput, throughput_stddev, threads, ratios, maxkeys, rqsizes, algs)
  readJavaResultsFileMemoryScalability(inputFileMemory, memory, memory_stddev, threads, algs)

  threads.sort()
  algs = [x for x in algs if 'Baseline' not in x]
  algs_tmp = []
  for alg in algs_order:
    if alg in algs:
      algs_tmp.append(alg)
  algs = algs_tmp
  print(graphtitle)
  # print(algs)
  # print(threads)
  # print(throughput)
  # print(graphtitle)
  # print(memory)

  plot_scalability_memory_graphs(inputFileThroughput, outputFile, graphtitle, throughput, throughput_stddev, memory, memory_stddev, threads, ratios, maxkeys, rqsizes, algs)

  print('max nodes traversed by remove: ' + str(max_nodes_traversed_by_remove))
