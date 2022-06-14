#!/usr/bin/env python3

import argparse
import errno
from aircrushcore.cms import Project,Subject,Session,Host
from aircrushcore.cms.project_collection import ProjectCollection
from aircrushcore.cms.subject_collection import SubjectCollection
from aircrushcore.cms.session_collection import SessionCollection
from aircrushcore.controller.configuration import AircrushConfig
import os,sys
from multiprocessing import Pool,cpu_count
import time

def main():
    global crush_host
    parser = argparse.ArgumentParser(
        description="Identify completed sessions")
    parser.add_argument('--project',action='store',type=str,
        help="Specify Project Name to focus on", required=True)
    parser.add_argument('--out',action='store',type=str,
        help="output filename to create", required=True)
    args = parser.parse_args()

    ########## Get connected ##########

    aircrush=AircrushConfig()
    crush_host=Host(
        endpoint=aircrush.config['REST']['endpoint'],
        username=aircrush.config['REST']['username'],
        password=aircrush.config['REST']['password']
    )
   
    
    ######## Get Sessions #########
    to_process=[]
    proj_coll=ProjectCollection(cms_host=crush_host)
    project=proj_coll.get_one_by_name(args.project)
    if project is not None:
        sess_coll=SessionCollection(cms_host=crush_host)
        print(f"Seeking sessions for project {project.uuid}")
        sessions=sess_coll.get(filter=f"&filter[proj-filter][condition][path]=field_participant.field_project.id&filter[proj-filter][condition][operator]=%3D&filter[proj-filter][condition][value]={project.uuid}")
        if sessions is None:
            print(f"No sessions found")
            return
        cnt_completed=0
        cnt_total=len(sessions)
        for session in sessions:            
            if sessions[session].field_status=='completed':
                cnt_completed=cnt_completed+1
                to_process.append([args.project,sessions[session].subject().title,sessions[session].title])

        print(f"Total sessions: {cnt_total}, completed: {cnt_completed} ({cnt_completed/cnt_total*100}%)")
    else:
        print(f"Project not found {args.project}")

    if len(to_process)==0:
        print(f"No completed sessions found for {args.project}")
        return
    else:
        parentdir=os.path.dirname(args.out)
        filename=os.path.basename(args.out)
        #If file is already being written, wait until done
        if os.path.exists(f"{parentdir}/.{filename}"):
            retries=6
            while retries>0:
                time.sleep(10)
                if os.path.exists(f"{parentdir}/.{filename}"):
                    retries=retries-1
                else:
                    retries=0
        if os.path.exists(f"{parentdir}/.{filename}"):
            print("Timeout expired waiting for file lock to complete")
            sys.exit(0)
        
        os.makedirs(parentdir,exist_ok=True)
        print(f"Writing {parentdir}/{filename}")
        f = open(f"{parentdir}/.{filename}", "w")
        for todo in to_process:
            f.write(f"{todo[0]},{todo[1]},{todo[2]}\n")
        f.close()
        if os.exists(f"{parentdir}/{filename}"):
            os.remove(f"{parentdir}/{filename}")
        os.rename(f"{parentdir}/.{filename}",f"{parentdir}/{filename}")
    
if __name__ == '__main__':
    main()


   
