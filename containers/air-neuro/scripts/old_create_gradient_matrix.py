#!/usr/bin/env python3
import pandas as pd
import os
import argparse


def main():
    args=None

    parser = argparse.ArgumentParser(
        description="CRUSH client command line utility. Converts BVEC file to gradient table for use in TravkVis")
    parser.add_argument('-diffusionpath',action='store',type=str,
        help="Parent directory of diffusion files where bvec file is found")
    parser.add_argument('-out',action='store',type=str,
        help="Specify a target filename for gradient table")

    args = parser.parse_args()

    bvec_fname=""
    dwifiles=os.listdir(args.diffusionpath)
    for f in dwifiles:
        if f.endswith('bvec'):
            bvec_fname=f"{args.diffusionpath}/{f}"
            break  #Get the first one I can find, we are only processing the first scan of this session
        if f=='bvecs':
            bvec_fname=f"{args.diffusionpath}/{f}"
            break
    if bvec_fname=="":
        raise Exception(f'No bvec file found in [{args.diffusionpath}].  Cannot establish gradient table.')
    
    csv = pd.read_csv(bvec_fname, skiprows=0,sep='\s+')
    df_csv = pd.DataFrame(data=csv)
    transposed_csv = df_csv.T        
    transposed_csv.to_csv(args.out,header=False,index=True)

if __name__ == '__main__':
    main()
