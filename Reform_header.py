#!/usr/bin/python3
####################################################################
#Reformats older illumina HiSeq fastqs to newer format
#Usage: Reform_header.py -f [infilenames] OPTIONS
#Mandatory argument -f --file, infiles
#Optional argument -p --processes, (default autodetct)
#Processing may be I/O limited on standard machines
#4 March 2021
#Maxim Seferovic, seferovi@bcm.edu
####################################################################

import argparse, os.path
import multiprocessing
from datetime import datetime

def timestamp(action, object):
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S') : <22}"
        f"{action : <12}"
        f"{object}"
        )
    
def filecheck (fname):
    if os.path.isfile(fname.rsplit('.', 1)[0] + '_fmt.' + fname.rsplit('.', 1)[-1]): 
        print ("Processed file exists, skipping\t", fname)
    else: return fname
      
def load_mod (n_processors, file):
    timestamp('Loading:', file)
    with open (file, 'r') as fp:
        line_chunks = split(fp.readlines(), n_processors)
    sname = file.rsplit('.', 1)[0] + '_fmt.' + file.rsplit('.', 1)[-1]
       
    timestamp('Processing:', file)
    return_dict = manager.dict()  
    new_line_chunks = []
    for count, i in enumerate(line_chunks):
        p = multiprocessing.Process(target=lprocess, args=(i, count, return_dict))
        new_line_chunks.append(p)
        p.start()
    for i in new_line_chunks: i.join()
        
    timestamp('Saving:', sname)
    with open (sname, 'w') as f:
        for i in return_dict.keys(): f.write("".join(return_dict[i]))

def split(a, n):  # function to split a list in n even parts
    k, m = divmod(len(a), n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def lprocess(lines, count, return_dict):
    new_lines = []
    for line in lines:
            if line[0] == '@':
                line = line.split()
                processed_line = line[0] + line[1][1:].strip() + "#0/" + line[1][0] +"\n"
            else: processed_line = line
            new_lines.append(processed_line)
    return_dict[count]= new_lines

def main (f,p):
    file_list = []
    for i in f: file_list.append(filecheck(i))
    file_list = list(filter(lambda x: x != None, file_list))
    print (f"Processing {len(file_list)} unprocessed files... ")
    for i, file in enumerate(file_list): 
        load_mod(p, file)
        if i % 10 == 0: print(f"File proccessing ~{int(i/len(file_list)*100)}% finished...")
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='in_file')
    parser.add_argument('-p',  '--processes', required=False, type=int, dest='processes', default=multiprocessing.cpu_count())
    args = parser.parse_args()
    manager = multiprocessing.Manager()
    main (args.in_file, args.processes)
