#!/usr/bin/python3

def preamble():
    return ("""
fastq_editor.py reformats fastq reads.

By default is updates older Illumina headers to a newer format, eg:  
   @E00380:590:HM7WWCCXY:6:1101:5578:1309 1:N:0:GCGTTAGA+TCTAACGC'   
to @E00380:590:HM7WWCCXY:6:1101:5578:1309:N:0:GCGTTAGA+TCTAACGC#0/1' 
Modify 'read_format()' (hihglighted in the script) to custom change reads                

Multiprocessing is active by default for configuratioss with high I/O speed like servers, the bypassing option is quicker for conventional configurations 
and/or for smaller files, and uses less memory. If availble memory is insufficient to multiprocess a large file, it is automatically single processed instead.
 
Last Updated: 10 March 2021
Maxim Seferovic, seferovi@bcm.edu

""")

def read_format(line):
### MODIFY THIS LINE TO CUSTOMIZE READ FORMATING ###
    return (''.join(re.split('\s[0-9]', line.replace('\n', f"#0/{line.split()[1][0]}\n" , 1))))
####################################################


import argparse, os.path, re
import multiprocessing, psutil
from datetime import datetime
from itertools import islice

def timestamp(action, object):
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S') : <22}"
        f"{action : <18}"
        f"{object}"
        )

def savename (fname):
    return fname.rsplit('.', 1)[0] + '_fmt.' + fname.rsplit('.', 1)[-1]

def mem_check(fname):
    return False if os.path.getsize(fname)*3 > psutil.virtual_memory()[1] else True
    
def filecheck (fname):
    if os.path.isfile(savename(fname)): print ("Processed file exists, skipping\t", fname) 
    else: return fname
    
def split(a, n):
    k, m = divmod(len(a), n)
    return len(a), list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def load_file (n_processors, file):
    timestamp('Loading:', file)
    with open (file, 'r') as fp:
        n, line_chunks = split(['@' + s for s in (fp.read()).split('@')[1:]] , n_processors)
    timestamp('Reads processing:', n)
    return_dict = manager.dict()
    new_line_chunks = []
    for count, i in enumerate(line_chunks):
        p = multiprocessing.Process(target=lprocess, args=(i, count, return_dict))
        new_line_chunks.append(p)
        p.start()
    for i in new_line_chunks: i.join()

    timestamp('Saving:', savename(file))
    count = 0
    with open (savename(file), 'w') as f:
        for i in sorted(return_dict.keys()): 
            f.write("".join(return_dict[i]))
            count += len(return_dict[i])
    timestamp('Reads saved:', count)

def lprocess(lines, count, return_dict):
    new_lines = []
    for line in lines: new_lines.append(read_format(line))
    return_dict[count]= new_lines

def single_proc(file):
    timestamp('Loading:', file)
    new_lines=[]
    with open (file, 'r') as fp:
        timestamp('Processig:', file)
        while True:
            read = ''.join(list(islice(fp, 4)))
            if not read: break
            new_lines.append(read_format(read))
    timestamp('Saving:', savename(file))
    with open (savename(file), 'w') as f:
        for i in new_lines: f.write(i)
    timestamp('Reads saved:', len(new_lines))

def main (f,p,m):
    file_list = []
    for i in f: file_list.append(filecheck(i))
    file_list = list(filter(lambda x: x != None, file_list))
    print (f"Processing {len(file_list)} unprocessed files... ")
    for i, file in enumerate(file_list):
        if m == True: single_proc(file)
        elif mem_check(file) == False: 
            print ('Not enough Memory single processing ' , file)
            single_proc(file) 
        else: load_file(p, file)
        if i % 10 == 0: print(f"File proccessing ~{int(i/len(file_list)*100)}% finished...")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=print(preamble()))
    parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='in_file')
    parser.add_argument(
        '-p',  '--processes', required=False, type=int, dest='processes', 
        help='Divide into n file chuncks to simultaneously process, defaults to autodetect CPUs', 
        default=multiprocessing.cpu_count()
        )
    parser.add_argument(
        '--bypass', required=False, action='store_true', dest='bypass', default=False,
        help='To bypass multiprocessing and single thread files one at a time'
        )
    args = parser.parse_args()
    manager = multiprocessing.Manager()
    main (args.in_file, args.processes, args.bypass)
