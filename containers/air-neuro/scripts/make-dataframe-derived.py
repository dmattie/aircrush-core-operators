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
                        if roi not in data[pipeline][subject][session]:continue
                        for roiend in data[pipeline][subject][session][roi]:
                            if roiend not in data[pipeline][subject][session][asymmetry]: continue
                            if roiend not in asym[pipeline][subject][session][asymmetry]: asym[pipeline][subject][session][asymmetry][roiend]={}
                            if roiend not in asym[pipeline][subject][session][roi]: asym[pipeline][subject][session][roi][roiend]={}
                            if roiend not in data[pipeline][subject][session][roi]:continue
                            for method in data[pipeline][subject][session][roi][roiend]:  
                                if method not in data[pipeline][subject][session][asymmetry][roiend]: continue
                                if method not in asym[pipeline][subject][session][asymmetry][roiend]:asym[pipeline][subject][session][asymmetry][roiend][method]={}
                                if method not in asym[pipeline][subject][session][roi][roiend]:asym[pipeline][subject][session][roi][roiend][method]={}
                                if method not in data[pipeline][subject][session][roi][roiend]:continue
                                for measurement in data[pipeline][subject][session][roi][roiend][method]:
                                    if measurement not in data[pipeline][subject][session][asymmetry][roiend][method]: continue  #If there is no counterpart to derive from... skip
                                    #print(f"roi:{roi}({data[pipeline][subject][session][roi][roiend][method][measurement]}) asymmetry:{asymmetry}({data[pipeline][subject][session][asymmetry][roiend][method][measurement]}) measurement:{measurement} ")
                                        
                                    v1=data[pipeline][subject][session][roi][roiend][method][measurement]                                        
                                    v2=data[pipeline][subject][session][asymmetry][roiend][method][measurement]
                                    if v1 is not None and v2 is not None:
                                        try:
                                            asymidx_v1_v2=float(v1)/float(v2) if float(v2)!=0 else 0   
                                            #asymidx_v2_v1=float(v2)/float(v1) if float(v1)!=0 else 0                                        
                                            key=f"{measurement}-asymidx"
                                            
                                            asym[pipeline][subject][session][roi][roiend][method][key]=asymidx_v1_v2
                                        except Exception as ex:
                                            print(f"Failed to determine asymmetry {pipeline}-{subject}-{session}-{roi}-{roiend}-{method} v1:{v1} v2:{v2}\n{ex}")
                                    
    
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
        
    

       
def main():
    global crush_host
    parser = argparse.ArgumentParser(
        description="Dataframe of CRUSH results")
    parser.add_argument('--df',action='store',type=str,
        help="crush dataframe file", required=True)    
    parser.add_argument('--segmentmap',action='store',type=str,help='Filename Location of segment map')    
    parser.add_argument('--overwrite',action='store_true',help='Overwrite destination if it exists')
    args = parser.parse_args()

    if os.path.exists(args.df):
        if args.overwrite:
            os.remove(args.df)
        else:
            print(f"Target already exists ({args.df})")
            return    
    
    add_derived_measurements(args.df,args.segmentmap)
    


#pdfoutput = "".join(outputs)

if __name__ == '__main__':
    main()


   
