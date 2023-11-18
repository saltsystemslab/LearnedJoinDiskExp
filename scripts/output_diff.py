#!/usr/bin/python3
import sys
import struct
import binascii

f1_path = sys.argv[1]
f2_path = sys.argv[2]


# Header 16 bytes
with open(f1_path, "rb") as f1, open(f2_path, "rb") as f2:
    f1_header = {
        "num_keys": struct.unpack('Q', f1.read(8))[0],
        "key_size": struct.unpack('I', f1.read(4))[0],
        "value_size": struct.unpack('I', f1.read(4))[0],
    }
    f2_header = {
        "num_keys": struct.unpack('Q', f2.read(8))[0],
        "key_size": struct.unpack('I', f2.read(4))[0],
        "value_size": struct.unpack('I', f2.read(4))[0],
    }
    if f1_header != f2_header:
        print("Headers not equal", f1, f2)
    num_keys = f1_header["num_keys"]
    tuple_size = f1_header["key_size"] + f1_header["value_size"]

    for i in range(f1_header["num_keys"]):
        f1_kv = f1.read(tuple_size)
        f2_kv = f2.read(tuple_size)
        if f1_kv != f2_kv:
            print(i)
            print(f1_path, binascii.hexlify(f1_kv))
            print(f2_path, binascii.hexlify(f2_kv))
            exit()

    pass 
