import os
import csv
import subprocess
size=[1000]
#size=[1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000]
disk=[0, 1]
use_128=[0,1]
use_bulk_copy=[0,1]
os.system("echo size,use_disk,use_int_128,use_string_keys,use_bulk_copy, PLR_training_time, merge_duration >> result1.txt")

for i in size:
    for j in disk:
        for k in use_128:
                for m in use_bulk_copy:
                    os.system("echo -n 1000,"+str(i)+ "," +str(j)+ "," +str(k)+ "," +str(m)+ ","">> result1.txt")
                    os.environ["USE_INT_128"] = str(k)
                    os.environ["USE_BULK_COPY"] = str(m)
                    os.system("make clean benchmark")
                    #dir='./bin/'

                    result = subprocess.run(["./bin/benchmark", '--num_keys=1000,'+str(i), '--use_disk='+str(j)], capture_output=True, text=True)
                    
                    #os.system("echo result >> result1.txt")
                    #csv_writer = csv.writer(open('output.csv', 'w', newline=''))

                    for line in result.stdout.splitlines():
                        if line.startswith("Merge duration"):
                            words = line.split()
                            duration= words[2].strip()
                            c = f'echo "{duration}"' '>> result1.txt'
                            os.system(c)
                        
                        # csv_writer.writerow([line])
                        # print ("stdout:", line)


    
                    #os.system("./bin/benchmark --num_keys=1000,"+str(i)+" --use_disk="+str(j)+" >> result1.txt")