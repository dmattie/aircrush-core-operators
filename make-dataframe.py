#!/usr/bin/env python3

import argparse
import errno
from tempfile import tempdir
from aircrushcore.cms import Project,Subject,Session,Host
from aircrushcore.cms.project_collection import ProjectCollection
from aircrushcore.cms.subject_collection import SubjectCollection
from aircrushcore.cms.session_collection import SessionCollection
from aircrushcore.controller.configuration import AircrushConfig
import os,sys
from multiprocessing import Pool,cpu_count
import tarfile
import uuid
import shutil

def getmeasurements(task_details):
    project=task_details[0]
    subject=task_details[1]
    session=task_details[2]
    dc=task_details[3]
    target=task_details[4]
    outputfile=task_details[5]
    
    
    key=f"{subject}/{session}"
    
    file=f"{dc}/projects/{project}/datasets/derivatives/levman/sub-{subject}/sub-{subject}_ses-{session}.tar"
    if not os.path.isfile(file):
        print(f"File not found ({file})")
        return
    else:
        try:
            tar = tarfile.open(file)
        except Exception as e:
            print(f"Error opening tarfile {file}\n{e}")
            return

        parentdir=os.path.dirname(file)
        os.makedirs(parentdir,exist_ok=True)

           
        important_files = [  tarinfo for tarinfo in tar.getmembers()
                if tarinfo.name.startswith(f"ses-{session}/crush.txt")
                ]
        
        tar.extractall(f"{target}/sub-{subject}",important_files)
        
        crushfile=f"{target}/sub-{subject}/ses-{session}/crush.txt"        
        return crushfile
    return None

            
def tmpdir(working_dir):
    
    found=False
    while found==False:
        dir=f"{working_dir}/temp_{str(uuid.uuid4())[:8]}"        
        if not os.path.isdir(dir):            
            try:
                os.makedirs(dir)
                found=True
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
    return dir
def main():
    global crush_host
    parser = argparse.ArgumentParser(
        description="Build dataframe of CRUSH results")
    parser.add_argument('--infile',action='store',type=str,
        help="Specify CSV containing list of completed sessions (project,subject,session).  E.g. adni,014S6437,S965218", required=True)
    parser.add_argument('--out',action='store',type=str,
        help="output filename to create dataframe", required=True)
    args = parser.parse_args()

    if os.path.exists(args.out):
        print(f"Target already exists ({args.out})")
        return

    aircrush=AircrushConfig()

    if aircrush.get_config_dtn()!="":
        print(f"The data commons appears to be stored on another cluster according to crush.ini COMMONS/data_transfer_node setting ({aircrush.get_config_dtn()}).  This code will only work on the cluster that houses the data commons.")
        return
    dc=aircrush.get_config_datacommons()

    working_dir=tmpdir(aircrush.get_config_wd())
    print(f"Staging directory: {working_dir}")
    
    ######## Get Sessions #########
    to_process=[]
    infile=args.infile
    f = open(infile, "r")
    cnt=0
    for ses in f:
        line=ses.split(',')
        to_process.append(line)
        cnt=cnt+1
        if cnt>4:
            break

    if len(to_process)==0:
        print(f"No completed sessions found for {args.project}")
        return
    makethese=[]
    for todo in to_process:
        print(todo[1])
        makethese.append([todo[0],todo[1],todo[2],dc,working_dir,args.out])
    
    ################
    # Get Tracts
    ################

    no_of_procs = os.getenv("SLURM_CPUS_PER_TASK")#cpu_count()    
    if no_of_procs is None:
        no_of_procs=1

    print(f"Multiprocessing {len(to_process)} tasks across {no_of_procs} CPUs")

    #with Pool(int(no_of_procs)) as p:
    #    p.map(getmeasurements,to_process)
    results = Pool(int(no_of_procs)).map(getmeasurements,to_process)

    crushfiles = [result for result in results]
    
    ################ APPEND TO DATAFRAME #################

    with open(args.out,"wb") as fout:
        for crushf in crushfiles:
            
            with open(crushf, "rb") as f:            
                fout.write(f.read())
    
    ################ CLEANUP ############################
    shutil.rmtree(working_dir)

#pdfoutput = "".join(outputs)

if __name__ == '__main__':
    main()


   
