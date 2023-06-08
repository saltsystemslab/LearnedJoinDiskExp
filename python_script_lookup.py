import os
import csv
import subprocess

no_of_items =[20000000]
no_of_queries=[80000000]
disk=[0, 1]
use_128=[0,1]
learned_lookup=[0,1]
os.system("echo no_of_items,no_of_queries,use_disk,use_int_128,lookup_type,duration >> result1.txt")
for a in no_of_items:
    
    for b in no_of_queries:
        
        
        for j in disk:
            for k in use_128:
                    

                    for m in learned_lookup:
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
                                            lookup = "standard lookup"
                                        else:
                                            lookup = "learned lookup"

                                    
                                        
                                        os.system("echo -n "+str(a)+"," ">>result1.txt")
                                        os.system("echo -n "+str(b)+"," ">>result1.txt")
                                        os.system("echo -n "+use_disk+","+use_128_bit+","+lookup+",>> result1.txt")
                                        os.environ["USE_INT_128"] = str(k)
                                        os.environ["USE_LEARNED_MERGE"] = str(m)
                                        os.system("make clean benchmark_lookup")
                                        #dir='./bin/'

                                        result = subprocess.run(["./bin/benchmark_lookup", '--num_keys='+str(a), '--num_queries='+str(b), '--use_disk='+str(j), "--universe_log="+universe_log], capture_output=True, text=True)
                                        
                                        #os.system("echo result >> result1.txt")
                                        #csv_writer = csv.writer(open('output.csv', 'w', newline=''))

                                        for line in result.stdout.splitlines():
                                            if line.startswith("Lookup duration"):
                                                words = line.split()
                                                duration= words[2].strip()
                                                c = f'echo "{duration}"' '>> result1.txt'
                                                os.system(c)
                                
                        # csv_writer.writerow([line])
                        # print ("stdout:", line)



                    #os.system("./bin/benchmark --num_keys=1000,"+str(i)+" --use_disk="+str(j)+" >> result1.txt")