#!/usr/bin/python3
####################################################################
#Separates out reads to component samples for illumina reads by name. 
#Usage: separate_reads_multithread.py [options][filenames.fna/fastq]
#Mandatory argument -f --file, files to parse
#Optional arguments -t --threads, defaults to 4
#                   -d --delimiter, defaults to whitespace & '-'
#30 Nov 2020
#Maxim Seferovic, seferovi@bcm.edu
####################################################################

import sys, os, argparse
from pathlib import Path
import multiprocessing

def load_data (args):
    print ('loading files...')
    loaded_data ={}
    for i in args: 
        fp = open(i)
        loaded_data[str(i)] = fp.readlines()
        fp.close()
    return loaded_data

def process_reads(n, f, de):
    print ("starting: ", n)    
    fdata = {}
    for line in f:
        if line[0] == '>': read = line
        elif len(line) >=3: fdata[read] = read+line
        else: continue  
    samples ={}
    for k,v in fdata.items():
        name = k.split()[0]
        name = name.split(de)[0]
        name = name.split('-')[0][1:].strip()
        if name in samples: samples[name].append(v)
        else: samples[name] = [v]
    return samples, n
     
def save_files(sorted_dict, fname, fd ):   
    Path(fd).mkdir(parents=True, exist_ok=True)
    fd = Path(fd)
    for key, value in sorted_dict.items():
        data = key + fname[-4:]
        save_file = fd/data
        f = open(save_file,'w')
        f.write((''.join(map(str, value))) )
        f.close()  
    print ('finsihed: ', fname) 

def main():
    wd = os.getcwd()
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='in_file')
    parser.add_argument('-t',  '--threads', required=False, type=int, dest='threads', default=4)
    parser.add_argument('-d',  '--delimiter', required=False, dest='delim', default=' ')
    args = parser.parse_args()
    argument_list = args.in_file
    
    data =  load_data(args.in_file)
    
    pool = multiprocessing.Pool(processes=args.threads)
    sorted_f = [pool.apply_async(
        process_reads, 
        args=(k, v, args.delim, ),
        callback=None
    ) for k,v in data.items()]
    pool.close()
    pool.join()
        
    for file in sorted_f:
        folder = wd + "/" + file.get()[1][:-4]
        save_files(file.get()[0], file.get()[1],folder)

if __name__ == "__main__":
    main()
