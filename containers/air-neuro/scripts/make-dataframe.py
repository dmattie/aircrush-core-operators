#!/usr/bin/env python3

import argparse
import errno
from re import S
from tempfile import tempdir
# from aircrushcore.cms import Project,Subject,Session,Host
# from aircrushcore.cms.project_collection import ProjectCollection
# from aircrushcore.cms.subject_collection import SubjectCollection
# from aircrushcore.cms.session_collection import SessionCollection
# from aircrushcore.controller.configuration import AircrushConfig
import os,sys
from multiprocessing import Pool,cpu_count
import tarfile
import uuid
import shutil
import copy

def get_segments(segmentmap_filename:str):
    segments=[]
    with open(segmentmap_filename, "r") as f:   
        rawsegments = [line.rstrip() for line in f]   
    for l in rawsegments:      
        #ParcellationId,Label,Asymmetry Counterpart,White Grey Cunterpart,Left or Right,White or Grey,Common Name
        #3027,wm-lh-rostralmiddlefrontal,4027,1027,Left,White,left rostral middle frontal white matter       
        # 
        if not l.startswith('#'):
            larr=l.split(',')        
            roi={'roi':larr[0],
                'label':larr[1],
                'asymmetry':larr[2],
                'whitegrey_counterpart':larr[3],
                'leftright':larr[4],
                'whitegrey':larr[5],
                'commonname':larr[6]
            }

            segments.append(roi)
    return segments

def add_derived_measurements(dataframe_filename:str, segmentmap_filename:str):
    segments=get_segments(segmentmap_filename)
    data={}
    f=open(dataframe_filename,'r')
    count=0
    while True:
        count+=1
        line=f.readline()
        if not line:
            break
        #levman,168S6142,S931159,3035,0255,roi_end,NumTracts,1830516
        larr=line.rstrip().split(',')
        
        pipeline=larr[0]
        subject=larr[1]
        session=larr[2]
        roistart=larr[3]
        roiend=larr[4]
        method=larr[5]
        measure=larr[6]
        value=larr[7]

        if pipeline not in data:  data[pipeline]={} 
        if subject not in data[pipeline]: data[pipeline][subject]={}
        if session not in data[pipeline][subject]: data[pipeline][subject][session]={}
        if roistart not in data[pipeline][subject][session]:data[pipeline][subject][session][roistart]={}
        if roiend not in data[pipeline][subject][session][roistart]:data[pipeline][subject][session][roistart][roiend]={}
        if method not in data[pipeline][subject][session][roistart][roiend]:data[pipeline][subject][session][roistart][roiend][method]={}        
        data[pipeline][subject][session][roistart][roiend][method][measure]=value

    #print(data['levman']['168S6142']['S931159']['3035']['0255']['roi_end']['NumTracts'])


    for pipeline in data:
        for subject in data[pipeline]:
            for session in data[pipeline][subject]:
                for roi in data[pipeline][subject][session]:
                    for roiend in data[pipeline][subject][session][roi]:
                        for method in data[pipeline][subject][session][roi][roiend]:
                            for measurement in data[pipeline][subject][session][roi][roiend][method]:   
                                val=data[pipeline][subject][session][roi][roiend][method][measurement]
                                #print(f"{pipeline}, {subject}, {session}, {roi}, {roiend}, {method}, {measurement}, {val}")   
    print("Looking for asymmetry values that can be derived")
    counter=0
    asym={}
    for segment in segments:        
        roi=segment['roi']
        asymmetry=segment['asymmetry']        
        if asymmetry is not None and asymmetry!="":
            for pipeline in data:
                if pipeline not in asym: asym[pipeline]={}
                for subject in data[pipeline]:
                    if subject not in asym[pipeline]: asym[pipeline][subject]={}
                    for session in data[pipeline][subject]:
                        if session not in asym[pipeline][subject]: asym[pipeline][subject][session]={}                                                                        
                        if asymmetry not in data[pipeline][subject][session]: continue                            
                        if asymmetry not in asym[pipeline][subject][session]: asym[pipeline][subject][session][asymmetry]={}
                        if roi not in asym[pipeline][subject][session]:asym[pipeline][subject][session][roi]={}
                        for roiend in data[pipeline][subject][session][roi]:
                            if roiend not in data[pipeline][subject][session][asymmetry]: continue
                            if roiend not in asym[pipeline][subject][session][asymmetry]: asym[pipeline][subject][session][asymmetry][roiend]={}
                            if roiend not in asym[pipeline][subject][session][roi]: asym[pipeline][subject][session][roi][roiend]={}                            
                            for method in data[pipeline][subject][session][roi][roiend]:  
                                if method not in data[pipeline][subject][session][asymmetry][roiend]: continue
                                if method not in asym[pipeline][subject][session][asymmetry][roiend]:asym[pipeline][subject][session][asymmetry][roiend][method]={}
                                if method not in asym[pipeline][subject][session][roi][roiend]:asym[pipeline][subject][session][roi][roiend][method]={}                                                                                                  
                                for measurement in data[pipeline][subject][session][roi][roiend][method]:
                                    if measurement not in data[pipeline][subject][session][asymmetry][roiend][method]: continue  #If there is no counterpart to derive from... skip
                                    #print(f"roi:{roi}({data[pipeline][subject][session][roi][roiend][method][measurement]}) asymmetry:{asymmetry}({data[pipeline][subject][session][asymmetry][roiend][method][measurement]}) measurement:{measurement} ")
                                        
                                    v1=data[pipeline][subject][session][roi][roiend][method][measurement]                                        
                                    v2=data[pipeline][subject][session][asymmetry][roiend][method][measurement]
                                    asymidx_v1_v2=float(v1)/float(v2) if float(v2)!=0 else 0   
                                    #asymidx_v2_v1=float(v2)/float(v1) if float(v1)!=0 else 0                                        
                                    key=f"{measurement}-asymidx"
                                    
                                    asym[pipeline][subject][session][roi][roiend][method][key]=asymidx_v1_v2
                                    
    
    for pipeline in asym:
        for subject in asym[pipeline]:
            for session in asym[pipeline][subject]:
                for roi in asym[pipeline][subject][session]:
                    for roiend in asym[pipeline][subject][session][roi]:
                        for method in asym[pipeline][subject][session][roi][roiend]:
                            for measurement in asym[pipeline][subject][session][roi][roiend][method]:   
                                val=asym[pipeline][subject][session][roi][roiend][method][measurement]
                                data[pipeline][subject][session][roi][roiend][method][measurement]=val
                                #print(f"@@{pipeline}, {subject}, {session}, {roi}, {roiend}, {method}, {measurement}, {val}")     

    with open(f"{dataframe_filename}.asymidx", "w") as f:   
        for pipeline in data:
            for subject in data[pipeline]:
                for session in data[pipeline][subject]:
                    for roi in data[pipeline][subject][session]:
                        for roiend in data[pipeline][subject][session][roi]:
                            for method in data[pipeline][subject][session][roi][roiend]:
                                for measurement in data[pipeline][subject][session][roi][roiend][method]:   
                                    val=data[pipeline][subject][session][roi][roiend][method][measurement]                                  
                                    f.write(f"{pipeline},{subject},{session},{roi},{roiend},{method},{measurement},{val}\n")    
        
    

def getmeasurements(task_details):
    print(task_details)
    project=task_details[0]
    subject=task_details[1]
    session=task_details[2]
    dc=task_details[3]
    target=task_details[4]  
    
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
    parser.add_argument('--datacommons',action='store',type=str,help='Directory to datacommons')
    parser.add_argument('--workingdir',action='store',type=str,help='Directory to workspace')
    parser.add_argument('--segmentmap',action='store',type=str,help='Filename Location of segment map')
    parser.add_argument('--overwrite',action='store_true',help='Overwrite destination if it exists')
    args = parser.parse_args()

    if os.path.exists(args.out):
        if args.overwrite:
            os.remove(args.out)
        else:
            print(f"Target already exists ({args.out})")
            return

    #aircrush=AircrushConfig()

    # if aircrush.get_config_dtn()!="":
    #     print(f"The data commons appears to be stored on another cluster according to crush.ini COMMONS/data_transfer_node setting ({aircrush.get_config_dtn()}).  This code will only work on the cluster that houses the data commons.")
    #     return
    dc=args.datacommons#aircrush.get_config_datacommons()

    working_dir=tmpdir(args.workingdir)#aircrush.get_config_wd())
    print(f"Staging directory: {working_dir}")
    
    ######## Get Sessions #########
    to_process=[]
    infile=args.infile
    f = open(infile, "r")
    cnt=0
    for ses in f:
        line=ses.strip('\n').split(',')
        to_process.append(line)
        cnt=cnt+1
        # if cnt>2:
        #     break

    if len(to_process)==0:
        print(f"No completed sessions found for {args.project}")
        return
    makethese=[]
    for todo in to_process:
        print(todo[1])
        makethese.append([todo[0],todo[1],todo[2],dc,working_dir])
    
    ################
    # Get Tracts
    ################

    no_of_procs = os.getenv("SLURM_CPUS_PER_TASK")#cpu_count()    
    if no_of_procs is None:
        no_of_procs=1

    print(f"Multiprocessing {len(to_process)} tasks across {no_of_procs} CPUs")

    #with Pool(int(no_of_procs)) as p:
    #    p.map(getmeasurements,to_process)
    results = Pool(int(no_of_procs)).map(getmeasurements,makethese)

    crushfiles = [result for result in results]
    
    ################ APPEND TO DATAFRAME #################

    with open(args.out,"wb") as fout:
        for crushf in crushfiles:
            if os.path.exists(crushf):
                with open(crushf, "rb") as f:            
                    fout.write(f.read())
            else:
                print(f"Missing file:{crushf}")
        
    ################ CLEANUP ############################
    shutil.rmtree(working_dir)

    add_derived_measurements(args.out,args.segmentmap)
    


#pdfoutput = "".join(outputs)

if __name__ == '__main__':
    main()


   
