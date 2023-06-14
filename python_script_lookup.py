import os
import csv
import subprocess

no_of_items =[100000000]
no_of_queries=[1000000000]
error = [10, 1000, 100000]
disk=[0, 1]
use_128=[0]
learned_lookup=[0,1, 2]
os.system("echo no_of_items,no_of_queries,use_disk,use_int_128,error_bound,lookup_type,training_duration,segment_count,lookup_duration >> result1.txt")
for a in no_of_items:
    
    for b in no_of_queries:
        
        
        for j in disk:
            for k in use_128:
                    
                for l in error:
                    for m in learned_lookup:
                                        os.environ["USE_BINARY_SEARCH"] = str(0)
                                        os.environ["USE_PLR_MODEL"] = str(0)
                                        os.environ["USE_PGM_INDEX"] = str(0)

                                        if(j==0):
                                            use_disk = "no disk"
                                        else:
                                            use_disk = "disk"
                                        
                                        if(k==0):
                                            use_128_bit = "64 bit"
                                            universe_log = "63"
                                        else:
                                            use_128_bit = "128 bit"
                                            universe_log = "127"

                                        if(m==0):
                                            lookup = "binary"
                                            os.environ["USE_BINARY_SEARCH"] = str(1)
                                        elif(m==1):
                                            lookup = "plr"
                                            os.environ["USE_PLR_MODEL"] = str(1)
                                        elif(m==2):
                                            lookup = "pgm"
                                            os.environ["USE_PGM_INDEX"] = str(1)

                                    
                                        
                                        os.system("echo -n "+str(a)+"," ">>result1.txt")
                                        os.system("echo -n "+str(b)+"," ">>result1.txt")
                                        os.system("echo -n "+use_disk+","+use_128_bit+","+str(l)+","+lookup+",>> result1.txt")
                                        os.environ["USE_INT_128"] = str(k)
                                        os.environ["ERROR_BOUND"] = str(l)
                                        os.system("make clean benchmark_lookup")
                                        #dir='./bin/'

                                        result = subprocess.run(["./bin/benchmark_lookup", '--num_keys='+str(a), '--num_queries='+str(b), '--use_disk='+str(j), "--universe_log="+universe_log], capture_output=True, text=True)
                                        
                                        #os.system("echo result >> result1.txt")
                                        #csv_writer = csv.writer(open('output.csv', 'w', newline=''))

                                        for line in result.stdout.splitlines():
                                            print(line)
                                            if line.startswith("Lookup duration"):
                                                words = line.split()
                                                duration= words[2].strip()
                                                c = f'echo -n "{duration}"' ',>> result1.txt'
                                                os.system(c)
                                            if line.startswith("Training duration:"):
                                            
                                                words = line.split()
                                                traning_duration= words[2].strip()
                                                c = f'echo -n "{traning_duration}"' ',>> result1.txt'
                                                os.system(c)
                                            if line.startswith("Number of segments:"):
                                                words = line.split()
                                                segments= words[3].strip()
                                                c = f'echo -n "{segments}"' ',>> result1.txt'
                                                os.system(c)
                                        c= f'echo ""''>>result1.txt'
                                        os.system(c)
                        # csv_writer.writerow([line])
                        # print ("stdout:", line)



                    #os.system("./bin/benchmark --num_keys=1000,"+str(i)+" --use_disk="+str(j)+" >> result1.txt")