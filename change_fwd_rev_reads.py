#!/usr/bin/python3
####################################################################
#Re-designates fwd/rev reads from fastq names
#Usage: change_fwd_rev.py -f [infilenames]
#Mandatory argument -f --file, infiles
#8 Dec 2020
#Maxim Seferovic, seferovi@bcm.edu
####################################################################

import argparse, re

def load_mod (arg):
    newfile = ''
    f_r = arg.split('_sequence_')[0][-1:]
    with open (arg, 'r') as fp:
        for l in fp: newfile += re.sub('\#.?', ('#%s' % f_r), l)
    return newfile

def save_files(out_data, fname):
    sname = fname.rsplit('.', 1)[0] + '_fr.' + fname.rsplit('.', 1)[-1]
    with open (sname,'w') as f: f.write(out_data)  
    print ('finsihed: ', sname) 

parser = argparse.ArgumentParser()
parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='in_file')
args = parser.parse_args()

for i in args.in_file: 
    data = load_mod(i)
    save_files(data, i)