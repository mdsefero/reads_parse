#!/usr/bin/python3
#######################
#This script adds a taxaID of choice to all instances of accesssion IDs in a fasta/fastq file 
#This is necessary for mapping when manually building Kraken libraries, for example if not downloaded form NCBI
#Usage:  taxid_append.py files
#Maxim Seferovic, seferovi@bcm.edu
#3 Dec 2020
#######################

import argparse

def parse(name):
    out = list()
    with open(name) as handle:
        for i in handle: 
            if i[0] == '>' : newline = i.strip() + '|kraken:taxid|%s\n' % taxid
            else: newline = i
            out.append(newline)
    return out
    
def save(data, f):
    savename = f.rsplit('.', 1)[0] + '_out.' + f.rsplit('.', 1)[-1]
    f = open(savename,'w')
    for values in data: f.write(values)
    f.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',  '--file', nargs ='+', required=True, type=str, dest='files')
    args = parser.parse_args()
    file_list = args.files
    
    global taxid
    taxid = input("Enter the desired TaxID #: ")
    for f in file_list: 
        newf = parse(f)
        save(newf, f)
        
if __name__ == "__main__":
    main()
