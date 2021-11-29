#!/usr/bin/env python3
import argparse
import pandas as pd
import os,sys


parser = argparse.ArgumentParser(
    description="CRUSH utility, converts bvecs to gradient matrix")
parser.add_argument('-bvec',action='store',required=True,
    help="Path to bvec file")
parser.add_argument('-out',action='store',required=True,
    help="Output filename for gradient matrix file")

args=parser.parse_args()

if not os.path.isfile(args.bvec):
    print(f"bvec file not found ({args.bvec})")
    sys.exit(1)

csv = pd.read_csv(args.bvec, skiprows=0,sep='\s+')
df_csv = pd.DataFrame(data=csv)
transposed_csv = df_csv.T        
transposed_csv.to_csv(args.out,header=False,index=True)

        