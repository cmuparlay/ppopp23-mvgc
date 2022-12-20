#!/bin/bash

for file in `ls results_aug9`
do
  cat results_aug7/${file} results_aug9/${file} > results_aug9_combined/${file}
done

# for file in `ls`|cut -d"-" -f1
# do
#   cat ${file}-* > ${file}
# done

