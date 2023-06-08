import os
import csv
import subprocess
no_of_items=[1000000]
size_skewness=[9, 99, 999, 9999, 99999]
# size = []
# for i in no_of_items:
#     for j in size_skewness:
#         l = i/(j+1)
#         r = l*j
#         size.append([l,r])

        


size=[]
#size=[1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000]
disk=[0, 1]
use_128=[0,1]
use_bulk_copy=[0,1]
trust_error_bounds = [0]
train_result = [0]
learned_merge=[0,1]
os.system("echo no_of_items,size_skewness,use_disk,use_int_128,use_bulk_copy,trust error bound,train result,merge type,merge_duration >> result1.txt")
for a in no_of_items:
    
    for b in size_skewness:
        
        l = a/(b+1)
        r = l*b
        for j in disk:
            for k in use_128:
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
                    os.environ["USE_LEARNED_MERGE"] = "0"
                    os.system("make clean benchmark")
                    os.system("echo -n "+str(a)+"," ">>result1.txt")
                    os.system("echo -n 1:"+str(b)+"," ">>result1.txt")
                    bulk_copy = "NA"
                    trust_error = "NA"
                    training_result = "NA"
                    merge = "standard"
                    os.system("echo -n "+use_disk+","+use_128_bit+","+bulk_copy+","+trust_error+","+training_result+","+merge+",>> result1.txt")

                    result = subprocess.run(["./bin/benchmark", '--num_keys='+str(l)+','+str(r), '--use_disk='+str(j), "--universe_log="+universe_log], capture_output=True, text=True)
                    for line in result.stdout.splitlines():
                        if line.startswith("Merge duration"):
                            words = line.split()
                            duration= words[2].strip()
                            c = f'echo "{duration}"' '>> result1.txt'
                            os.system(c)

                    for m in use_bulk_copy:
                            for n in trust_error_bounds:
                                for o in train_result:
                                        os.system("echo -n "+str(a)+"," ">>result1.txt")
                                        os.system("echo -n 1:"+str(b)+"," ">>result1.txt")
                                        
                                        

                                        if(m==0):
                                            bulk_copy = "no bulk copy"
                                        else:
                                            bulk_copy = "bulk copy"

                                    
                                        if(n==0):
                                            trust_error = "no trust_error_bounds"
                                        else:
                                            trust_error = "trust_error_bounds"

                                        if(o==0):
                                            training_result = "no train_result"
                                        else:
                                            training_result = "train_result"

                                        merge = "learned merge"

                                        os.system("echo -n "+use_disk+","+use_128_bit+","+bulk_copy+","+trust_error+","+training_result+","+merge+",>> result1.txt")
                                        os.environ["USE_INT_128"] = str(k)
                                        os.environ["USE_BULK_COPY"] = str(m)
                                        os.environ["TRUST_ERROR_BOUNDS"] = str(n)
                                        os.environ["TRAIN_RESULT"] = str(o)
                                        os.environ["USE_LEARNED_MERGE"] = "1"
                                        os.system("make clean benchmark")
                                        #dir='./bin/'

                                        result = subprocess.run(["./bin/benchmark", '--num_keys='+str(l)+','+str(r), '--use_disk='+str(j), "--universe_log="+universe_log], capture_output=True, text=True)
                                        
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