import json
import sys
from .session import Session
from .session_collection import SessionCollection
from .task_instance import TaskInstance
from .task_collection import TaskCollection
from .task_instance_collection import TaskInstanceCollection
#from .compute_node_collection import ComputeNodeCollection
from .pipeline_collection import PipelineCollection
import subprocess
from humanfriendly import format_size, parse_size
from ..common import Readiness


HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKCYAN = '\033[96m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

class ComputeNode():
   
    def __init__(self,**kwargs):


        self.title=""                        
        self.field_host=""                
        self.field_username=""
        self.__field_password=""
        self.field_working_directory=""   
        self.field_account="" 
        self.body=""
        self.uuid=""  
        self.HOST=None            
        

        #TODO  Need log and error locations, job start directory       
      
        if 'metadata' in kwargs:
            m=kwargs['metadata']

        if 'title' in m:
            self.title=m['title']
        if 'field_host' in m:
            self.field_host=m['field_host']      
        if 'field_username' in m:            
            self.field_username=m['field_username']                     
        if 'field_password' in m:
            self.__field_password=m['field_password']
        if 'field_working_directory' in m:            
            self.field_working_directory=m['field_working_directory']
        if 'field_account' in m:
            self.field_account=m['field_account']
        if 'body' in m:
            self.body=m['body']     
        if "uuid" in m:
            self.uuid=m['uuid']          
        if "cms_host" in m:
            self.HOST=m['cms_host']         
        
        self.aircrush=m['aircrush'] if 'aircrush' in m else None 
        
       


    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def upsert(self):

            payload = {
                "data" : {
                    "type":"node--compute_node",                      
                    "attributes":{
                        "title": self.title,                                                                            
                        "field_host":self.field_host,
                        "field_username":self.field_username,
                        "field_password":self.__field_password,
                        "field_working_directory":self.field_working_directory,    
                        "field_account":self.field_account,                                            
                        "body":self.body                        
                    }                    
                }
            }
            #print(json.dumps(payload))
            if self.uuid:   #Update existing                     
                payload['data']['id']=self.uuid                                             
                r= self.HOST.patch(f"jsonapi/node/compute_node/{self.uuid}",payload)
            else:                
                r= self.HOST.post("jsonapi/node/compute_node",payload)

            if(r.status_code!=200 and r.status_code!=201):                   
                print(f"[ERROR] failed to upsert ComputeNode {self.uuid} ({self.title}) on CMS HOST: {r.status_code},  {r.reason}\n\n{payload}")
            else:   
                self.uuid =r.json()['data']['id']      
                return r.json()['data']['id']          
              
    def delete(self):
        if self.uuid:          
            r= self.HOST.delete(f"jsonapi/node/compute_node/{self.uuid}")
        else: 
            return False
        
        if(r.status_code!=204):                   
            raise ValueError(f"ComputeNode deletion failed [{self.uuid}]")

        return True
       
    def _sizeof_fmt(self,num, suffix="B"):
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f}Yi{suffix}"
        
    def _prereq_concurrency(self):
        print("Assessing concurrency threshold...",end='',flush=True)
        if self.aircrush is None:
            print(f"{OKGREEN}[PASSED]{ENDC} Skipping concurrency check, config settings not passed")
            return True
        if self.aircrush.config.has_option('COMPUTE','concurrency_limit'):            
            allocated=self.allocated_sessions()
            try:
                concurrency_limit=int(self.aircrush.config['COMPUTE']['concurrency_limit'])
                if concurrency_limit<=len(allocated):
                    print(f"{WARNING}[LIMITED]{ENDC} Concurrency limit reached. {len(allocated)} sessions already allocated.")
                    return False
                else:
                    print(f"{OKGREEN}[PASSED]{ENDC} Node can accept {concurrency_limit-len(allocated)} more sessions.  {len(allocated)} allocated, {concurrency_limit} maximum.")
                return True
            except Exception as e:
                print(f"{FAIL}[FAILED]{ENDC} Unable to assess concurrency limit {e}")
                return False
        else:
            print("{OKGREEN}[PASSED]{ENDC} No concurrency threshold set.")
            return True
        
        
    def _prereq_diskspace(self):   
        print("Assessing available disk....",end='',flush=True)
        if self.aircrush is None:
            print(f"{OKGREEN}[PASSED]{ENDC} Skipping disk check, config settings not passed")
            return True      

        if self.aircrush.config.has_option('COMPUTE','disk_used_cmd'):
            disk_used_cmd=self.aircrush.config['COMPUTE']['disk_used_cmd']
        else:
            disk_used_cmd=None
                
        if self.aircrush.config.has_option('COMPUTE','disk_quota_cmd'):
            disk_quota_cmd=self.aircrush.config['COMPUTE']['disk_quota_cmd']
        else:
            disk_quota_cmd=None


        if self.aircrush.config.has_option('COMPUTE','minimum_disk_to_act'):
            minimum_disk_to_act=self.aircrush.config['COMPUTE']['minimum_disk_to_act']
        else:
            minimum_disk_to_act=None


        if disk_used_cmd is None or disk_quota_cmd is None or minimum_disk_to_act is None:            
            print("{WARNING}[PASSED]{ENDC}: There are insufficient settings to determine disk space availability.  See disk_used_cmd, disk_quota_cmd, and minimum_disk_to_act settings.  Assuming no guardrails")
            return True  #No guardrails!           
        else:
            
            shell_cmd=["bash","-c",disk_used_cmd]     
            process = subprocess.Popen(shell_cmd, stdout=subprocess.PIPE)
            disk_used,_ = process.communicate(timeout=90)

            if process.returncode!=0:
                print(f"{FAIL}[FAILED]{ENDC} Failed to determine disk space in use({process.returncode}). Attempted:{disk_used_cmd}, Result: {disk_used}")                
                return False
            
            shell_cmd=["bash","-c",disk_quota_cmd]     
            process = subprocess.Popen(shell_cmd, stdout=subprocess.PIPE)
            disk_quota,_ = process.communicate(timeout=90)

            if process.returncode!=0:
                print(f"{FAIL}[FAILED]{ENDC} Failed to determine disk quota. Attempted:{disk_used_cmd}, Result: {disk_quota}.")                
                return False


            try:
                requirement=parse_size(minimum_disk_to_act)
            except Exception as e:
                self.eprint(f"{FAIL}Unable to decipher size specified in crush.ini [COMPUTE] minimum_disk_to_act option ({minimum_disk_to_act}){ENDC}")
                self.eprint(e)
                return False

            try:                
                disk_used_int=int(disk_used)
            except Exception as e:
                self.eprint("{FAIL}Value returned to determine used disk space was not a number ({disk_used}){ENDC}")
                self.eprint(e)
                return False
            try:                
                disk_quota_int=int(disk_quota)
            except Exception as e:
                self.eprint("{FAIL}Value returned to determine disk quota was not a number ({disk_quota}){ENDC}")
                self.eprint(e)
                return False
            
            found = disk_quota_int - disk_used_int                
            if found<requirement:
                print(f"{FAIL}[FAILED]{ENDC}: Prerequisite not met: Insufficient disk space to run this operation, {requirement} required, {found} available.")
                return False
            else:

                print(f"{OKGREEN}[PASSED]{ENDC} Diskspace looks good.  {self._sizeof_fmt(requirement)} required, {self._sizeof_fmt(found)} available.")
                return True

        return True
                    
    def isReady(self,skip_tests:bool=False):
        if skip_tests:
            return Readiness.READY
        else:
            if self._prereq_diskspace() == False:
                return Readiness.NOT_READY
            if self._prereq_concurrency() == False:
                return Readiness.LIMITED  

        return Readiness.READY

    def allocated_sessions(self):
        
        sess_col = SessionCollection(cms_host=self.HOST)   
        try:            
            sess_uids = sess_col.get(filter=f"&filter[field_responsible_compute_node.id][value]={self.uuid}")            
        except Exception as e:
            print(f"{FAIL}[ERROR]{ENDC} Unable to determine sessions allocated to this compute node.")
            self.eprint(e)
        if sess_uids is None:
            return {}
        else:
            return sess_uids
        #sess_uids = sess_col.get()
    def allocate_session(self,session_uuid:str):
        if self.uuid is None:
            raise Exception("ComputeNode is new.  Save before allocation of sessions")
        #Get Session
        sess_col = SessionCollection(cms_host=self.HOST)            
        sess = sess_col.get_one(uuid=session_uuid)

        # if sess.sticky==False: #not currently being allocated elsewhere
        #     sess.sticky=True  #Let's lock it up so it doesn't get allocated elsewhere
        #     sess.upsert()
        # else:
        #     subject=sess.subject()

        #     print("---------------------------------------------------------------------------")
        #     print(f"Attempted to allocate {subject.title}/{sess.title}, but session currently undergoing lock operation another compute node.  Check session sticky flag if problem continues")
        #     print("---------------------------------------------------------------------------")
        #     raise Exception("Session currently locked by another compute node")

        self._attach_to_session(session=sess)
        self._establish_task_instances(session=sess)

        sess.sticky=False
        sess.upsert()
    def refresh_task_instances(self):
        print("Refreshing task instances")
        sess_col = SessionCollection(cms_host=self.HOST)            
        sessions = sess_col.get(filter=f"&filter[field_responsible_compute_node.id][value]={self.uuid}&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=NOT%20IN&filter[status-filter][condition][value][1]=completed&filter[status-filter][condition][value][2]=processed&filter[status-filter][condition][value][3]=terminal")
        for ses in sessions:
            #print(f"\t{sessions[ses].title}")            
            self._establish_task_instances(sessions[ses])

    def _attach_to_session(self,session:Session):
    #    pass
        if session.field_responsible_compute_node is None:
            session.field_responsible_compute_node=self.uuid
            print(f"session {session.title} attached to {session.field_responsible_compute_node}")
            session.upsert
        else:
            print(f"Failed to attach {self.title} to session {session.title}")
        # else:
        #     cn_coll = ComputeNodeCollection(cms_host=self.HOST)
        #     cn=cn_coll.get_one(session.field_responsible_compute_node)
        #     if cn is None:
        #         subject=session.subject()
        #         raise Exception("Session {subject.title}/{session.title} is allocated to a compute node that no longer exists.  Task instances may be orphaned")
        #     else:                
        #         raise Exception("Session is already allocated to a compute node")
        
    def _establish_task_instances(self,session:Session):
        

        #Look at existing task instances
        ti_col = TaskInstanceCollection(cms_host=self.HOST,session=session.uuid)
        pipe_col = PipelineCollection(cms_host=self.HOST)
        tis = ti_col.get()

        ses_assigned_to_ti=None
        for ti in tis:
            #TIs exist for this session, let's see if they are with us or elsewhere
            #Get first task instance to determine allocated compute node               
            #ti = ti_col.get_one(uuid=ti_uuids[0])            
            ses_assigned_to_ti=tis[ti].field_associated_participant_ses
            break                 

        #If there are task instances assigned to this session or there are no task instances...
        #print(f"\tsession_assigned_to_ti:{ses_assigned_to_ti}, session:{session.uuid}")
        if ses_assigned_to_ti is None or ses_assigned_to_ti==session.uuid:
            project = session.project()
            if project == None:
                print(f"WARNING: Session {session.title} has been assigned to this compute node, but is orphaned or project is unpublished")
                return
           
            activated_pipelines = session.project().field_activated_pipelines
            
        #    print(f"\t\tActivated pipelines for this session: {activated_pipelines}")
            

            for pipeline_uuid in activated_pipelines:
                
                pipe = pipe_col.get_one(uuid=pipeline_uuid)
        #        print(f"\t\tPipeline: {pipe.title} ({pipeline_uuid})")
                task_col = TaskCollection(cms_host=self.HOST,pipeline=pipeline_uuid)
                for task_uuid in task_col.get():
        #            print(f"\t\t\ttask uuid:{task_uuid}")
                                      
                    #Look for a task instance for this pipeline.task associated with this session
                   ##This caused a TIMEOUT ti_col = TaskInstanceCollection(cms_host=self.HOST,pipeline=pipeline_uuid,task=task_uuid,session=session.uuid)
                    ti_col = TaskInstanceCollection(cms_host=self.HOST,session=session.uuid)
                    matching_tis = ti_col.get()
                    ti_exists=False
                    for match in matching_tis:
                        if matching_tis[match].field_pipeline==pipeline_uuid and matching_tis[match].field_task==task_uuid:
                            ti_exists=True
                    
                    if not ti_exists:
        #                print("\t\t\tCreate TI")
                        #Create this task instance
                        task = task_col.get_one(task_uuid)
                        pipeline = task.pipeline()#pipe_col.get_one(uuid=pipeline_uuid)
                        subject = session.subject()
        #                print("c")
                        metadata={
                            "title":f"{pipeline.title}/{task.title} ({task.field_operator}) on {subject.title}/{session.title}",
                            "field_associated_participant_ses":session.uuid,
                            "field_pipeline":pipeline_uuid,                                                                       
                            "field_task":task_uuid,
                            "cms_host":self.HOST  
                        }
        #                print(metadata)
                        ti = TaskInstance(metadata=metadata)
                        ti_uid=ti.upsert()
        #                print("TI upserted")
        #                print("e")


        else:
            raise Exception(f"Session is found on another compute node")
        

    

