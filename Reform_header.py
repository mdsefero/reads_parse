#!/usr/bin/python3
####################################################################
#Reformats older illumina HiSeq fastqs to newer format
#Usage: Reform_header.py -f [infilenames]
#Mandatory argument -f --file, infiles
#22 Feb 2021
#Maxim Seferovic, seferovi@bcm.edu
####################################################################

import argparse

def load_mod (arg):
    newfile = ''
    with open (arg, 'r') as fp:
        for l in fp:
            if l[0] == '@':
                l = l.split(' ')
                newfile += l[0] + l[1][1:].strip() + "#0/" + l[1][0] +"\n"
            else: newfile += l
    return newfile

def save_files(out_data, fname):
    sname = fname.rsplit('.', 1)[0] + '_fmt.' + fname.rsplit('.', 1)[-1]
    with open (sname,'w') as f: f.write(out_data)  
    print ('finsihed: ', sname) 

parser = argparse.ArgumentParser()
parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='in_file')
args = parser.parse_args()

for i in args.in_file: 
    data = load_mod(i)
    save_files(data, i)