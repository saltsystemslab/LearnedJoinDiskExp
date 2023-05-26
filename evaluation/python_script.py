import os
size=[1000]
#size=[1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000]
disk=[0, 1]
use_128=[0,1]
use_string = [0,1]
use_bulk_copy=[0,1]
for i in size:
    for j in disk:
        for k in use_128:
            for l in use_string:
                for m in use_bulk_copy:
                    os.system("echo size:1000,"+str(i)+ ", echo use_disk:"+str(j)+", echo use_int_128:"+str(k)+", echo use_string:"+str(l)+", echo use_bulk_copy:"+str(m)+" >> result1.txt")
                    os.environ["USE_INT_128"] = str(k)
                    os.environ["USE_STRING_KEYS"] = str(l)
                    os.environ["USE_BULK_COPY"] = str(m)
                    os.system("make clean benchmark")
                    os.system("./bin/benchmark --num_keys=1000,"+str(i)+" --use_disk="+str(j)+" >> result1.txt")