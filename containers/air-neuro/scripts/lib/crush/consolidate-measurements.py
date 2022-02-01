#!/usr/bin/env python3
 
import sys,os,subprocess
import re
import nibabel as nib
import numpy as np
import warnings
import json
import argparse


def process(**kwargs):#segment,counterpart,method):
        datasetdir=kwargs['datasetdir']
        subject=kwargs['subject']
        session=kwargs['session']
        pipeline=kwargs['pipeline']
        tidy=kwargs['tidy']
        clean=kwargs['clean']

        if not os.path.isdir(datasetdir):
            print(f"datasetdir not found: {datasetdir}")
            sys.exit(1)
        for dir in os.walk(f"{datasetdir}/derivatives/{pipeline}/crush"):
            print(dir)

def main():

    args=None

    parser = argparse.ArgumentParser(
        description="CRUSH client command line utility. Consolidate extracted measurements.  Clean up temp files.")
    parser.add_argument('-datasetdir',action='store', required=True, help="Path to dataset directory (just above ../[source|rawdata|derivatives]/..)")
    parser.add_argument('-subject',action='store', required=True, help="Specify Subject ID")
    parser.add_argument('-session',action='store', required=True, help="Specify Session ID")    
    parser.add_argument('-pipeline',action='store', required=True, help="The name of the pipeline being processed to tag the data as it is stored")    
    parser.add_argument('-tidy',action='store_true', help="If specified, tar temp files into a single file" )
    parser.add_argument('-clean',action='store_true', help="If specified, remove intermediate files (.nii, .txt, .json)" )
    
    args = parser.parse_args()

    process(datasetdir=args.datasetdir,subject=args.subject,session=args.session,pipeline=args.pipeline,tidy=args.tidy,clean=args.clean)

if __name__ == '__main__':
    main()
