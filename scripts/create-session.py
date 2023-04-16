import argparse
from pickle import FALSE
from aircrushcore.cms import compute_node, compute_node_collection, session_collection, task_instance_collection
from aircrushcore.controller.configuration import AircrushConfig
from aircrushcore.dag import Workload
from aircrushcore.cms import *
from aircrushcore.datacommons.data_commons import DataCommons
from util.setup import ini_settings

crush_host=None


def get_subject_uuid(project_uuid,subject_title):
    
    subj_col = SubjectCollection(cms_host=crush_host,project=project_uuid)
    subjects = subj_col.get()
    for subject in subjects:        
        subs_with_uuid=subj_col.get(uuid=subject)        
        if(len(subs_with_uuid))>0:
            for s in subs_with_uuid:
                if subject_title==subs_with_uuid[s].title:  
                    return s
    return None
def get_session_uuid(subject_uuid,session_title):
    
    sess_col = SessionCollection(cms_host=crush_host,subject=subject_uuid)
    sessions = sess_col.get()
    for session in sessions:        
        sess_with_uuid=sess_col.get(uuid=session)
        if(len(sess_with_uuid))>0:
            for s in sess_with_uuid:
                if session_title==sess_with_uuid[s].title:  
                    return s
    return None    
def create_session(subject_uuid,session_title):

    session_metadata={
            "title":args.session,            
            "field_participant":subject_uuid,
            "uuid":None,
            "field_status":"notstarted",            
            "cms_host":crush_host            
        } 
    s = Session(metadata=session_metadata)                
    session_uuid=s.upsert()
    print(f"new session created {session_uuid}")

def main():

    global aircrush,crush_host,args
    print("Aircrush Create utility")
    parser = argparse.ArgumentParser(
        description="CRUSH client command line utility. Back-end commands to create crush data")
    parser.add_argument('--subject',action='store',type=str,
        help="Subject ID to create (without the sub-). E.g. -subject 12345")
    parser.add_argument('--session',action='store',type=str,
        help="Session ID to create (without the ses-). E.g. -session 12345.")        
    parser.add_argument('--project',action='store',required=True,
        help='Project name of existing project' )
    args = parser.parse_args()

    aircrush=ini_settings()

    crush_host=Host(
        endpoint=aircrush.config['REST']['endpoint'],
        username=aircrush.config['REST']['username'],
        password=aircrush.config['REST']['password']
    )

    proj_col=ProjectCollection(cms_host=crush_host)
    proj = proj_col.get_one_by_name(args.project)
    if proj is None:
        print(f"Project ({args.project}) not found")
    else:
        print(f"[OK] Found project {args.project}, UUID {proj.uuid}")    
    subject_uuid = get_subject_uuid(project_uuid=proj.uuid,subject_title=args.subject)
    
    if subject_uuid is not None:
        if args.session is None:
            print(f"[FAIL] Subject {args.subject} exists: {subject_uuid}")
        else:
            session_uuid=get_session_uuid(subject_uuid=subject_uuid,session_title=args.session)
            if session_uuid is not None:                
                print(f"[FAIL] Session {args.subject}/{args.session} exists: {session_uuid}")
            else:
                session_uuid=create_session(subject_uuid,args.session)
    else:
        subject_metadata={
            "title":args.subject,
            "isbids":True,
            "field_project":proj.uuid,
            "uuid":None,
            "field_status":"notstarted",            
            "cms_host":crush_host            
        } 
        s = Subject(metadata=subject_metadata)                
        subject_uuid=s.upsert()
        print(f"new subject {subject_uuid}")
        
        session_uuid=create_session(subject_uuid,args.session)



#     if args.project_uuid is None:
#         print("Project UUID")

#     if args.subject is not None:
#         set_ti_status(uuid=args.uuid,status=args.value)
#     if args.set == "ti-memory-multiplier":
#         set_ti_memory_multiplier(uuid=args.uuid,val=args.value)        
#     else:
#         print("Nothing to do")

# def set_ti_status(uuid:str, status:str):
#     ti_col=TaskInstanceCollection(cms_host=crush_host)
#     if ti_col is not None:
#         ti=ti_col.get_one(uuid)        
#         print(f"{ti.title}\n\tOld Status:{ti.field_status}\n\tNew Status:{status}")
#         ti.field_status=status
#         ti.upsert()        
#         print("Update Complete")
# def set_ti_memory_multiplier(uuid:str, val:str):
#     ti_col=TaskInstanceCollection(cms_host=crush_host)
#     if ti_col is not None:
#         ti=ti_col.get_one(uuid)  
#         print(f"{ti.title}\n\tOld Memory Multiplier:{ti.field_multiplier_memory}\n\tNew Memory Multiplier:{val}")      
#         ti.field_multiplier_memory=val
#         ti.upsert()        
#         print("Update Complete")        

if __name__ == '__main__':
    main()

