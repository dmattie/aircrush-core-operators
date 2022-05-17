from aircrushcore.cms.task_instance_collection import TaskInstanceCollection
from aircrushcore.cms.task_instance import TaskInstance
from aircrushcore.controller.configuration import AircrushConfig
#from aircrushcore.cms.host import Host
from aircrushcore.cms import Host,Task,TaskCollection,ComputeNodeCollection,ComputeNode,SessionCollection,ProjectCollection,SubjectCollection
from aircrushcore.compute.compute_node_connection import ComputeNodeConnection
from aircrushcore.compute.compute import Compute
import subprocess
import urllib

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKCYAN = '\033[96m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

class Workload:
    def __init__(self,aircrush:AircrushConfig):            
        self.aircrush=aircrush 
        self.crush_host=Host(
            endpoint=aircrush.config['REST']['endpoint'],
            username=aircrush.config['REST']['username'],
            password=aircrush.config['REST']['password']
            )  
        try:
            self.concurrency_limit=int(aircrush.config['COMPUTE']['concurrency_limit'])
        except Exception as e:
            print("Configuration setting concurrency_limit in COMPUTE section not found or not an integer. Defaulting to 2.")
            self.concurrency_limit=2

        try:
            self.seconds_between_failures=int(aircrush.config['COMPUTE']['seconds_between_failures'])
        except Exception as e:
            print("Configuration setting seconds_between_failures in COMPUTE section not found or not an integer. Defaulting to 18000 (5 hours).")
            self.seconds_between_failures=18000
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def get_running_tasks(self,node_uuid:str):
        #Look for tasks initiated by this compute node(node-uuid)
        #Used to determine job status
        tis_on_this_node={}
        compute_node_coll = ComputeNodeCollection(cms_host=self.crush_host)
        compute_node = compute_node_coll.get_one(uuid=node_uuid)

        filter="sort[sort_filter][path]=field_status&sort[sort_filter][direction]=DESC&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=IN&filter[status-filter][condition][value][1]=running&filter[status-filter][condition][value][2]=limping"
        tic = TaskInstanceCollection(cms_host=self.crush_host)        
        tic_col = tic.get(filter=filter)
        for ti in tic_col:
            if tic_col[ti].associated_session().field_responsible_compute_node==node_uuid:
                tis_on_this_node[ti]=tic_col[ti]
        return tis_on_this_node

    def get_next_task(self,node_uuid:str, nth=1,accept_new_sessions=True):
        ###################################################################
        # Big fancy optimization AI brain goes here #######################
        ###################################################################
        #... but for now...
        #What node am I on?
        nth_decrementor=nth

        compute_node_coll = ComputeNodeCollection(cms_host=self.crush_host)
        compute_node = compute_node_coll.get_one(uuid=node_uuid)
        if compute_node is None:
            self.eprint(f"{FAIL}[ERROR]{ENDC} This compute node has not been defined in the CMS.")
            return
        #Ensure sessions are allocated and task instances reflect the current pipeline
        if accept_new_sessions==True:
            self._distribute_sessions_to_node(compute_node)
        else:
            print("{WARNING}No new sessions will be allocated to this node{ENDC}.  Only existing sessions will be advanced.")
       
        #Get task instances ready to run
        filter="sort[sort_filter][path]=field_status&sort[sort_filter][direction]=DESC&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=IN&filter[status-filter][condition][value][1]=failed&filter[status-filter][condition][value][2]=notstarted&filter[status-filter][condition][value][3]=waiting"
        #print(filter)
        tic = TaskInstanceCollection(cms_host=self.crush_host)        
        tic_col = tic.get(filter=filter)
        print(f"Sifting through incomplete task instances ({len(tic_col)}) for any allocated to this compute node")
        if(len(tic_col)>0):
            #Iterate the tasks, looking for one we can do
            for ti_idx in tic_col:
                
                ti = tic_col[ti_idx]                
                session = ti.associated_session()    
                
                if session.field_responsible_compute_node == node_uuid: # This session has been allocated to the node asking for work
                    print(f"Candidate task instance {ti.title} ...",end='')
  
                    if not self.unmet_dependencies(ti): #Ignore any with unmet dependencies

                        #If 
                        tises=ti.associated_session()
                        if not tises == None:
                            tisessub=tises.subject()
                            if not tisessub == None:
                                tisessubproj=tisessub.project()                                
                                if tisessubproj == None:
                                    print('.', end='')
                                    continue
                               
                        if ti.field_jobid and ti.field_status=='failed':
                           #Let's see if this has failed long enough ago that we can go again
                            duration=self.duration_since_job_end(ti.field_jobid)
                            if duration>self.seconds_between_failures:
                                print(f"{OKGREEN}recently failed ({self.seconds_between_failures} seconds ago).  Re-attempting...{ENDC}")
                                nth_decrementor = nth_decrementor - 1
                                if nth_decrementor==0:
                                    return ti
                                else:
                                    continue
                            else:
                                print(f"{ti.title} recently failed and will not be re-attempted until {self.seconds_between_failures} seconds have elapsed after failure.  See ~/.crush.ini ")
                        else:
                            nth_decrementor = nth_decrementor - 1
                            if nth_decrementor==0:
                                return ti
                            else:
                                print()
                                continue
                # else:
                #     print(f"skipping {ti.title}...session allocated to another node")
    def duration_since_job_end(self,jobid):
        # This command gets the seconds since unix epoch of job end and
        # seconds since unix epoch for now to get seconds between now and job failure
        cmd=f"expr $( date +%s ) - $( date \"+%s\" -d $( sacct -j {jobid}|head -3|tail -1|cut -c 55-74 ) )"
        
        code,out = self.getstatusoutput(cmd)
        if code==0:
            try:
                return int(out)
            except:
                return -1
        else:
            print(f"Failed to determine duration since job end of job id {jobid}")
        return -1
        
    
    def getstatusoutput(self,command):
        #print(command)    
        
        process = subprocess.Popen(command, stdout=subprocess.PIPE,shell=True)
        out, _ = process.communicate()
        return (process.returncode, out)

    def unmet_dependencies(self,candidate_ti:TaskInstance):
        task=candidate_ti.task_definition()
        session=candidate_ti.associated_session()
        if session is None:
            raise Exception(f"{FAIL}[ERROR]{ENDC} Task instance appears to be orphaned.  No associated session.")
             
        session_uuid=session.uuid
        for prereq_task in task.field_prerequisite_tasks:
            #for the given task, find dependent tasks with incomplete task instances 

            
            #Look for any incomplete task_instances matching this task_uuid for this session
            filter="&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=NOT%20IN&filter[status-filter][condition][value][1]=completed&filter[status-filter][condition][value][2]=processed"
            tic = TaskInstanceCollection(cms_host=self.crush_host,task=prereq_task['id'],session=session_uuid)        
            tic_col = tic.get(filter=filter)
            #print(f"\t{len(tic_col)} task instances instantiated for the same session that have failed or not started (including the candidate)")
            if not tic_col == None:
                for ti_uuid in tic_col:
                               
                    if ti_uuid != candidate_ti.uuid:
                        print(f"\t{WARNING}Prereq {tic_col[ti_uuid].field_status}{ENDC}:{tic_col[ti_uuid].title}")                         
                        return True
        #print("\tno unmet dependencies for the task definition associated with this task instance")
        return False


    def _distribute_sessions_to_node(self,compute_node:ComputeNode):
        if compute_node is None:
            raise 
        allocated_sessions = compute_node.allocated_sessions()
        
        if int(self.concurrency_limit)<=len(allocated_sessions):            
            print(f"\tCompute node at capacity ({self.concurrency_limit} sessions). See ~/.crush.ini to increase limits.")
            #return
        else:

            # Starting at projects that have sticky bit turned on, iterate projects to find sessions not yet allocated
        
            proj_col = ProjectCollection(cms_host=self.crush_host)
            outstanding_projects = proj_col.get(filter="&sort=-sticky") #Get sticky first (note the "-"" for descending)

            for outstanding_project in outstanding_projects:
                print(f"Checking the following project for sessions to process: {outstanding_projects[outstanding_project].title}")
                #Starting at subjects that are sticky (priority), iterate looking for sessions not yet allocated
                #    
                sub_col = SubjectCollection(cms_host=self.crush_host,project=outstanding_project)                
                #outstanding_subjects = sub_col.get(filter="&filter[field_status][value]=notstarted&sort=-sticky")
                outstanding_subjects = sub_col.get(filter="&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=NOT%20IN&filter[status-filter][condition][value][1]=completed&filter[status-filter][condition][value][2]=processed&filter[status-filter][condition][value][3]=terminal&sort=-sticky")
                print(f"\t{len(outstanding_subjects)} subjects not started")
                for outstanding_subject in outstanding_subjects:
                    if self.concurrency_limit<=len(allocated_sessions):
                        break
                    ses_col = SessionCollection(cms_host=self.crush_host,subject=outstanding_subject)
                        #Get sessions that don't have a compute node allocated
                 #   outstanding_sessions = ses_col.get(page_limit=2,filter="&filter[field_status][value]=notstarted")
                    #filter="&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=NOT%20IN&filter[status-filter][condition][value][1]=completed&filter[status-filter][condition][value][2]=processed"
                    outstanding_sessions = ses_col.get(page_limit=2,filter="&filter[status-filter][condition][path]=field_status&filter[status-filter][condition][operator]=NOT%20IN&filter[status-filter][condition][value][1]=completed&filter[status-filter][condition][value][2]=processed&filter[status-filter][condition][value][3]=terminal")
                 
                    print(f"\t\t{outstanding_subjects[outstanding_subject].title} has {len(outstanding_sessions)}  incomplete sessions")
                    for ses_uid in outstanding_sessions:
                        session=ses_col.get_one(ses_uid)
                        if session.field_responsible_compute_node is None:
                            subject=session.subject()
                            project=subject.project()

                            if subject == None or project == None:
                                print(f"Session {session.title} is orphaned, please conduct a health check.  Skipping")
                                continue

                            print(f"Allocating {project.title}/{subject.title}/{session.title}")
                            compute_node.allocate_session(session_uuid=session.uuid)                
                            allocated_sessions = compute_node.allocated_sessions()
                            if self.concurrency_limit<=len(allocated_sessions):
                                break

        compute_node.refresh_task_instances()
        

    def count_of_incomplete_tasks(self):
        tic = TaskInstanceCollection(cms_host=self.crush_host)
        tic_col = tic.get()
        return len(tic_col)

    def invoke_task(self,task_instance:TaskInstance):
        task = task_instance.task_definition()
        session = task_instance.associated_session()
        subject = session.subject()
        project = subject.project()               
        
        workers = ComputeNodeCollection(cms_host=self.crush_host).get(filter=f"filter[field_host][value]={project.field_host}")

        print(f"{len(workers)} worker nodes found matching host {project.field_host}")
        for worker_uuid in workers:
            worker=workers[worker_uuid]
            print(worker)
            if worker.isReady:
                print(f"Invoking Task {task.field_operator} on host {worker.field_host}")
                print(f"type======== {type(project.field_host)}")
                conn=ComputeNodeConnection(hostname=project.field_host,username=project.field_username,password=project.field_password)
                node=Compute(conn)
                #response = node.invoke(container="abc",command="whoami")

                

                #return response
        print("No worker nodes ready to perform this task instance")