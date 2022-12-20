
# zipfian key distribution

# Figure 4
python3 run_tree_experiments_rqsize.py 100000 40 40 40 [256,8192,65536,200000] graphs/Figure4.png 5 5 64G 0.99 $1

# Figure 5
python3 run_tree_experiments_rqsize.py 10000000 40 40 40 [1024,65536,1048576,20000000] graphs/Figure5.png 5 15 64G 0.99  $1

# Figure 6
python3 run_hashtable_experiments_rqsize.py 100000 40 40 40 [256,8192,65536,200000] graphs/Figure6.png 5 5 64G 0.99  $1



# Figure 7a
python3 run_tree_experiments_scalability.py 100000 25-25-49-1-1024 [1,60,120,180,240] graphs/Figure7a.png  5 5 64G 0.99  $1

# Figure 7b
python3 run_tree_experiments_scalability.py 10000000 25-25-49-1-1024 [1,60,120,180,240] graphs/Figure7b.png  5 15 64G 0.99  $1

# Figure 8
python3 run_hashtable_experiments_scalability.py 100000 25-25-49-1-1024 [1,60,120,180,240] graphs/Figure8.png  5 5 64G 0.99  $1



# uniform key distribution (Appendix)

# Figure 12
# python3 run_tree_experiments_rqsize.py 100000 40 40 40 [256,8192,65536,200000] graphs/Figure12.png 5 5 64G 0  $1

# Figure 13
# python3 run_tree_experiments_rqsize.py 10000000 40 40 40 [1024,65536,1048576,20000000] graphs/Figure13.png 5 15 64G 0  $1

# Figure 14
# python3 run_hashtable_experiments_rqsize.py 100000 40 40 40 [256,8192,65536,200000] graphs/Figure14.png 5 5 64G 0  $1



# Figure 15a
# python3 run_tree_experiments_scalability.py 100000 25-25-49-1-1024 [1,60,120,180,240] graphs/Figure15a.png  5 5 64G 0  $1

# Figure 15b
# python3 run_tree_experiments_scalability.py 10000000 25-25-49-1-1024 [1,60,120,180,240] graphs/Figure15b.png  5 15 64G 0  $1

# Figure 16
# python3 run_hashtable_experiments_scalability.py 100000 25-25-49-1-1024 [1,60,120,180,240] graphs/Figure16.png  5 5 64G 0  $1
