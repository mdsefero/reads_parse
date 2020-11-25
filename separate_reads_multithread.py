#!/usr/bin/python3
####################################################################
#Separates out reads to component samples for illumina reads by name. 
#Usage: separate_reads_multithread.py [options][filenames.fna/fastq]
#Mandatory argument -f --file, files to parse
#Optional arguments -t --threads, defaults to 4
#                   -d --delimiter, defaults to whitespace & '-'
#25 Nov 2020
#Maxim Seferovic, seferovi@bcm.edu
####################################################################

import sys, os, argparse
from pathlib import Path
import multiprocessing

def process_reads(f, de):
    print ("starting: ", f)
    handle = open(f)
    samples = {}
    read = ""
    for line in handle:
        if line[0] == '>':
            if read != "":
                if name in samples.keys(): samples[name] = samples[name] + read
                else: samples.update({name:read})
            name = line.split()[0]
            name = name.split(de)[0]
            name = name.split('-')[0][1:].strip()
            read = line
        else: read += line
    return samples, str(f)   
    handle.close()
        
def save_files(sorted_dict, fname, fd ):   
    Path(fd).mkdir(parents=True, exist_ok=True)
    fd = Path(fd)
    for key, value in sorted_dict.items():
        data = key + fname[-4:]
        save_file = fd/data
        f = open(save_file,'w')
        f.write (value)
        f.close()  
    print ('finsihed: ', fname)

def main():
    wd = os.getcwd()
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='in_file')
    parser.add_argument('-t',  '--threads', required=False, dest='threads', default=4)
    parser.add_argument('-d',  '--delimiter', required=False, dest='delim', default=' ')
    args = parser.parse_args()
    argument_list = args.in_file

    pool = multiprocessing.Pool(processes=args.threads)
    sorted_f = [pool.apply_async(
        process_reads, 
        args=(file,args.delim, ),
        callback=None
    ) for file in args.in_file]
    pool.close()

    for file in sorted_f:
        folder = wd + "/" + file.get()[1][:-4]
        save_files(file.get()[0], file.get()[1],folder)

if __name__ == "__main__":
    main()