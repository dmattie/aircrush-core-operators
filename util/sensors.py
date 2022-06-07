from ast import Name
from aircrushcore.dag import Workload
from aircrushcore.cms import Session, Subject, Project,SessionCollection,SubjectCollection,ProjectCollection,Task,TaskInstance,TaskInstanceCollection,ComputeNode,ComputeNodeCollection
import subprocess
import os
from . import config
from . import setup
from . import data_transfer
from .color import ansi
import datetime
import re

def check_recurring_pipelines(node_uuid,**kwargs):
    if 'aircrush' in kwargs:
        aircrush=kwargs['aircrush']
    else:
        aircrush=setup.ini_settings()
    if 'cms_host' in kwargs:
        crush_host=kwargs['cms_host']  
    else:              
        crush_host=config.get_cms_host()

    pass
def is_slurm_script(jobfile:str):
    regex = re.compile("^#SBATCH",re.IGNORECASE)
    with open(jobfile) as f:
        for line in f:
            result = regex.search(line)
            if result:
                return True
    return False


def exists_on_datacommons(data_transfer_node,path):
    if data_transfer_node is None or data_transfer_node=="":
        #Local
        if os.path.exists(path):
            return True
    else:        
        ret = subprocess.run(f"ssh {data_transfer_node} [ -f {path} ]",   
                                 capture_output=False,                                 
                                 shell=True,                               
                                 timeout=60)               
        if ret.returncode==0: 
            return True  
        else:   
            ret = subprocess.run(f"ssh {data_transfer_node} [ -d {path} ]",   
                            capture_output=False,                                 
                            shell=True,                               
                            timeout=60)     
            if ret.returncode==0:
                return True                    
    return False

def get_slurm_id(stdout:str):
    print(f"given {stdout}")
    lines=stdout.split('\n')
    if len(lines)>0:
        if lines[0][:20]=="Submitted batch job ":
            tokens=lines[0].split()            
            jobid=tokens[-1]
            return jobid
    return 0
def get_seff_completion_state(stdout:str):
    
    lines=stdout.split('\n')
    for line in lines:
        if line[:6]=="State:":
            tokens=line.split()            
            status=tokens[1]
            return status            
    return None
    
def check_running_jobs(node_uuid,**kwargs):
    if 'aircrush' in kwargs:
        aircrush=kwargs['aircrush']
    else:
        aircrush=setup.ini_settings()
    if 'cms_host' in kwargs:
        crush_host=kwargs['cms_host']  
    else:              
        crush_host=config.get_cms_host()

    # try: aircrush
    # except NameError: 
    # try: crush_host
    # except NameError: 

    w=Workload(aircrush)
    tis =w.get_running_tasks(node_uuid)
    active_tis=len(tis)
    reviewed_tis=active_tis
    if active_tis>0:
        print(f"Checking for status on {active_tis} jobs thought to be running on this compute node.")
    for ti in tis:        
        if tis[ti].field_jobid:
            print(f"Checking job {tis[ti].field_jobid} ({tis[ti].title})...",end='')            
            seff_cmd=['seff',f"{tis[ti].field_jobid}"]           
            try:
                ret = subprocess.run(seff_cmd,   
                                capture_output=True,
                                text=True,                                
                                timeout=60)                 
                if ret.returncode==0:
                    status=get_seff_completion_state(ret.stdout)                    
                    if status=='COMPLETED':                        
                        tis[ti].field_seff=ret.stdout
                        if tis[ti].field_logfile and os.path.isfile(tis[ti].field_logfile):
                            logfile = open(tis[ti].field_logfile,'r')

                            log_contents = logfile.read()
                            if len(log_contents)>2000:
                                log_contents=f"log file has been truncated.  see output log for complete detail\n\n{log_contents[-2000:]}"                            
                            tis[ti].body=log_contents                        
                        tis[ti].field_status="completed"
                        tis[ti].upsert()  
                        print(f"{ansi.OKGREEN}{status}{ansi.ENDC}")                        
                    elif status=="OUT_OF_MEMORY":
                        print(f"{ansi.FAIL}{status}{ansi.ENDC} ",end='')
                        if tis[ti].field_logfile and os.path.isfile(tis[ti].field_logfile):
                            logfile = open(tis[ti].field_logfile,'r')

                            log_contents = logfile.read()
                            if len(log_contents)>2000:
                                log_contents=f"log file has been truncated.  see output log for complete detail\n\n{log_contents[-2000:]}"                            
                            tis[ti].body=log_contents
                                                
                        if tis[ti].field_multiplier_memory is None:
                            tis[ti].field_multiplier_memory=1.5
                        else:
                            tis[ti].field_multiplier_memory=float(tis[ti].field_multiplier_memory)+0.5
                        print(f"\tAllocated memory was exhausted.  Extending to {tis[ti].field_multiplier_memory} times specified memory allocation.")  
                        tis[ti].field_status="failed"
                        tis[ti].upsert()                           

                    elif status=='FAILED' or status=="TIMEOUT" or status=="CANCELLED" or status=="NODE_FAIL":
                        print(f"{ansi.FAIL}{status}{ansi.ENDC}",end='')
                        if status=="TIMEOUT":
                            if tis[ti].field_multiplier_duration is None:
                                tis[ti].field_multiplier_duration=1.5                                
                            else:
                                tis[ti].field_multiplier_duration=float(tis[ti].field_multiplier_duration)+0.5
                            print(f"\tAllocated time was exhausted.  Extending to {tis[ti].field_multiplier_duration} times specified wall time.")                        
                        if status=="CANCELLED":
                            #Check for Out-of-memory
                            if _ti_oom(ret.stdout)==True:
                                if tis[ti].field_multiplier_memory is None:
                                    tis[ti].field_multiplier_memory=1.5
                                else:
                                    tis[ti].field_multiplier_memory=tis[ti].field_multiplier_memory+0.5
                                print("\tAllocated memory was exhausted.  Extending to {tis[ti].field_multiplier_memory} times specified memory allocation.")
                            
                        if tis[ti].field_errorfile and os.path.isfile(tis[ti].field_errorfile):
                            logfile = open(tis[ti].field_errorfile,'r')
                            error_contents = logfile.read()
                            if len(error_contents)>2000:
                                error_contents=f"log file has been truncated.  see error log for complete detail\n\n{error_contents[-2000:]}"
                            now = datetime.datetime.now()
                            nowstr=now.strftime("%m/%d/%Y, %H:%M:%S")
                            datestamp =f"##############################\n{nowstr}\n##############################"
                            tis[ti].field_errorlog=f"{datestamp}{error_contents}{tis[ti].field_errorlog}"
                            print("########  error log stored")         
                                             
                        else:
                            print(f"Error file not found ({tis[ti].field_errorfile})")
                        
                        if tis[ti].field_remaining_retries is None or tis[ti].field_remaining_retries=="":
                            tis[ti].field_remaining_retries=5
                            tis[ti].field_seff=ret.stdout
                            tis[ti].field_status="failed"                            
                        else:
                            if tis[ti].field_remaining_retries==0:
                                tis[ti].field_seff=ret.stdout
                                tis[ti].field_status="halted"                                
                                print("Too many failed retries.  This job will not continue without manual intervention")
                            else:
                                tis[ti].field_remaining_retries-=1
                                tis[ti].field_seff=ret.stdout
                                tis[ti].field_status="failed"
                        tis[ti].upsert()  
                                
                        print(f"##### Remaining retries: {tis[ti].field_remaining_retries}")
                    elif status=='RUNNING' or status=='PENDING':
                        print(f"{ansi.OKCYAN}{status}{ansi.ENDC} Nothing will be done.")                               
                    else: #UNRECOGNIZED SEFF STATUS
                        print(f"{ansi.WARNING}UNKNOWN SEFF STATUS ({status}){ansi.ENDC} Nothing will be done.")                               
                reviewed_tis=reviewed_tis-1
            except Exception as e:                
                raise Exception(f"{ansi.FAIL}[ERROR]{ansi.ENDC} Failed to execute seff, {e}")
    if reviewed_tis > 0:
        print(f"{reviewed_tis} jobs not accounted for")
    else:
        print("All running jobs on this node accounted for and updated in CMS")
def _ti_oom(seff_stdout):
    lines=seff_stdout
    for line in lines:
        tokens=line.split(' ')
        if len(tokens)>=3 and tokens[0]=="Memory" and tokens[1]=='Efficiency:':
            mem_percent=tokens[2].replace('%','')
            try:
                mem=float(mem_percent)
                if mem>90:
                    return True #MAybe cancelled due to OOM - this is faster than parsing logfile
            except:
                continue
    return False

def count_session_ti_states(session:Session,crush_host):
    # try: aircrush
    # except NameError: aircrush=setup.ini_settings()
    # try: crush_host
    # except NameError: crush_host=config.get_cms_host()

    states={'completed':0,'processed':0,'running':0,'failed':0,'waiting':0,'limping':0,'notstarted':0,'terminal':0}
    pipelines={}

    sub=session.subject()
    proj=sub.project()

    ti_col=TaskInstanceCollection(cms_host=crush_host,session=session.uuid)
    tis_for_session=ti_col.get()
    for ti in tis_for_session:
        if tis_for_session[ti].field_status=='completed':
            states['completed']+=1
        elif tis_for_session[ti].field_status=='processed':
            states['processed']+=1            
        elif tis_for_session[ti].field_status=='running':
            states['running']+=1
        elif tis_for_session[ti].field_status=='failed':
            if proj.field_treat_failed_as_terminal==False:
                states['failed']+=1 
            else:
                states['terminal']+=1 
        elif tis_for_session[ti].field_status=='waiting':
            states['waiting']+=1
        elif tis_for_session[ti].field_status=='limping':
            if proj.field_treat_failed_as_terminal==False:
                states['limping']+=1    
            else:
                states['terminal']+=1             
        elif tis_for_session[ti].field_status=='terminal':
            states['terminal']+=1
        else:
            states['notstarted']+=1

        if tis_for_session[ti].field_pipeline:
            pipelines[tis_for_session[ti].field_pipeline]=tis_for_session[ti].pipeline()
    return states,pipelines


def cascade_status_to_subject(node_uuid,**kwargs):
    if 'aircrush' in kwargs:
        aircrush=kwargs['aircrush']
    else:
        aircrush=setup.ini_settings()
    if 'cms_host' in kwargs:
        crush_host=kwargs['cms_host']
    else:        
        crush_host=config.get_cms_host()

    node_col=ComputeNodeCollection(cms_host=crush_host);
    node=node_col.get_one(uuid=node_uuid)
    print(f"Reviewing compute node {node.title}")
    attached_sessions=node.allocated_sessions()
    if aircrush.config.has_option('COMPUTE','concurrency_limit'):
        concurrency_limit = aircrush.config['COMPUTE']['concurrency_limit']
    else:
        concurrency_limit=1000    
    print(f"{ansi.UNDERLINE}({len(attached_sessions)}/{concurrency_limit}) sessions allocated to this compute node.  Updating status based on task instances...{ansi.ENDC}")
    
    subjects_of_attached_sessions={}
    for session_uuid in attached_sessions:

        session=attached_sessions[session_uuid]        

        states,pipelines = count_session_ti_states(session,crush_host=crush_host)

        count_running=states['running']
        count_failed=states['failed']
        count_completed=states['completed']
        count_notstarted=states['notstarted']
        count_processed=states['processed']
        count_waiting=states['waiting']
        count_limping=states['limping']
        count_terminal=states['terminal']
        derived_status=derive_session_status(count_failed,count_running,count_completed,count_notstarted,count_processed,count_waiting,count_limping,count_terminal)        
        session.field_status=derived_status
        subject=session.subject()                        

        subjects_of_attached_sessions[subject.uuid]=subject

        project=subject.project()        

        if subject == None or project == None:
            if subject is None:
                subject_msg=f"{ansi.WARNING}NO SUBJECT{ansi.ENDC}"
            else:
                subject_msg=subject.title

            if project is None:
                project_msg=f"{ansi.WARNING}NO PROJECT{ansi.ENDC}"
            else:
                project_msg=project.title

            print(f"\tSession {session.title} is orphaned, please conduct a health check.\n\tSubject:{subject_msg}\n\tProject:{project_msg}  Skipping")
            continue
        print(f"Synchronizing {project.title}:{subject.title}/{session.title} with status {ansi.OKCYAN}{session.field_status}{ansi.ENDC} (failed:{count_failed} running:{count_running} completed:{count_completed} not started:{count_notstarted} processed:{count_processed} waiting:{count_waiting} limping:{count_limping} terminal:{count_terminal})")
        if session.field_status=='processed' or session.field_status=='completed' or session.field_status=='terminal':
            data_transfer.commit(project.title,subject.title,session.title if session is not None else "")
            if session.field_status=='processed':
                session.field_status='completed'
            cn=session.field_responsible_compute_node
            session.field_responsible_compute_node=None #Free up a slot on compute node for more
            print(f"{subject.title}.{session.title} deallocated from {cn}.  New allocation:{session.field_responsible_compute_node}")

        session.upsert()        
    print(f"{ansi.UNDERLINE}Updating subject status based on session status...{ansi.ENDC}")
    for subject in subjects_of_attached_sessions:
        count_running=0
        count_failed=0
        count_completed=0
        count_notstarted=0
        count_processed=0
        count_waiting=0
        count_limping=0
        count_terminal=0

        ses_col=SessionCollection(cms_host=crush_host,subject=subject)
        sessions_for_subject=ses_col.get()
        for sess in sessions_for_subject:
            if sessions_for_subject[sess].field_status=='completed':
                count_completed+=1
                continue
            if sessions_for_subject[sess].field_status=='running':
                count_running+=1
                continue
            if sessions_for_subject[sess].field_status=='failed':
                count_failed+=1
                continue
            if sessions_for_subject[sess].field_status=='waiting':
                count_waiting+=1
                continue
            if sessions_for_subject[sess].field_status=='limping':
                count_limping+=1
                continue    
            if sessions_for_subject[sess].field_status=='terminal':
                count_terminal+=1
                continue
            count_notstarted+=1
        project=subjects_of_attached_sessions[subject].project()

        derived_status=derive_subject_status(count_failed,count_running,count_completed,count_notstarted,count_processed,count_waiting,count_limping,count_terminal)
        print(f"Subject ({project.title}/{subjects_of_attached_sessions[subject].title}) status set to {ansi.OKCYAN}{derived_status}{ansi.ENDC} (failed:{count_failed} running:{count_running} completed:{count_completed} not started:{count_notstarted} processed:{count_processed} waiting: {count_waiting}, limping:{count_limping} terminal:{count_terminal})")
        subjects_of_attached_sessions[subject].field_status=derived_status
        subjects_of_attached_sessions[subject].upsert()

def derive_session_status(failed,running,completed,notstarted,processed,waiting,limping,terminal):
    if terminal>0:
        return "terminal"
    if processed > 0 and failed==0 and running==0 and completed==0 and notstarted==0 and limping==0:
        #All session operations are done for this subject, time to push up to data commons
        return "processed"
    if processed > 0 and failed==0 and running==0 and completed==0 and limping==0 and notstarted>0:
        #All session operations are done for this subject, time to push up to data commons
        return "waiting"     
    if limping >0:
        return "limping"   

    if failed>0:
        if notstarted+running+processed+completed==0:
            return "failed"
        else:
            return "limping"

    if running>0:
        if failed>0:
            return "limping"
        if failed==0:
            return "running"

    if completed>0:
        if notstarted==0:
            return "completed"
        else:
            return "waiting"
    if waiting>0:
        return "waiting"

    return "notstarted"
def derive_subject_status(failed,running,completed,notstarted,processed,waiting,limping,terminal):
    no_life_left=terminal+completed+processed
    some_life_left=failed+running+notstarted+waiting+limping
    
    if some_life_left>0:

        if processed > 0 and failed==0 and running==0 and completed==0 and notstarted==0 and limping==0:
            #All session operations are done for this subject, time to push up to data commons
            return "processed"
        if processed > 0 and failed==0 and running==0 and completed==0 and limping==0 and notstarted>0:
            #All session operations are done for this subject, time to push up to data commons
            return "waiting"     
        if limping >0:
            return "limping"   

        if failed>0:
            if notstarted+running+processed+completed==0:
                return "failed"
            else:
                return "limping"

        if running>0:
            if failed>0:
                return "limping"
            if failed==0:
                return "running"
    if no_life_left>0:
        if completed>0:
            if notstarted==0:
                return "completed"
            else:
                return "waiting"
        if waiting>0:
            return "waiting"
        if terminal>0:
            return "terminal"

    return "notstarted"
    