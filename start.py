#!/usr/bin/env python3

import argparse
from pickle import FALSE
from aircrushcore.cms import compute_node, compute_node_collection, session_collection, task_instance_collection
from aircrushcore.controller.configuration import AircrushConfig
from aircrushcore.dag import Workload
from aircrushcore.cms import *
from aircrushcore.datacommons.data_commons import DataCommons
from os.path import exists,dirname
import os,sys
import importlib
import getpass
#from operators import invoke_operator
import datetime
import ast
import subprocess
import argparse
import socket
from humanfriendly import format_size, parse_size

aircrush=None
crush_host=None
args=None

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKCYAN = '\033[96m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

def ready():
    #Sense readiness to do something
    return True

def print_statusline(msg: str):
    last_msg_length = len(print_statusline.last_msg) if hasattr(print_statusline, 'last_msg') else 0
    print(' ' * last_msg_length, end='\r')
    print(msg, end='\r')
    sys.stdout.flush()  # Some say they needed this, I didn't.
    print_statusline.last_msg = msg
       
def getOperatorClassDefinition(task_uuid:str):
    
    #This function uses the task uuid to load the associated operator module 
    #and return a class defintion

    task = TaskCollection(cms_host=crush_host).get_one(task_uuid) #fetch task definition
    operator=task.field_operator #identify the operator to execute
    module=f"{task.field_operator}_operator" #build filename for dynamic load
    operator_module=importlib.import_module(f"operators.{module}")  #dynamically import identified operator
    op_class=getattr(operator_module,operator) #get class defintion    
    return op_class

def getMyComputeNodeUUID():

    cluster=aircrush.config['COMPUTE']['cluster']
    account=aircrush.config['COMPUTE']['account']    
    working_dir=aircrush.config['COMPUTE']['working_directory'] #os.environ.get("SCRATCH")    
    username=getpass.getuser()


    metadata={
        "title":f"{cluster}/{username}",
        "field_host":cluster,
        "field_username":username,
        "field_password":"",
        "field_working_directory":working_dir,        
        "cms_host":crush_host,
        "aircrush":aircrush   
    }

    #Look for known match in cms and create if not there
    cn_col = ComputeNodeCollection(cms_host=crush_host)
    matching_cn = cn_col.get(filter=f"&filter[field_username][value]={username}&filter[field_host][value]={cluster}")

    for match in matching_cn:          
        metadata['uuid']=match
        break

    n = ComputeNode(metadata=metadata)    
    nuid=n.upsert()
    
    #Is this node ready for work?
    readystate=n.isReady()    
    return nuid,readystate


def pullContainer(uri:str):
    #return "requirements.txt"   ##TODO
    if (args.container):
        
        return args.container

    container_dir=aircrush.config['COMPUTE']['singularity_container_location']

    sif = f"{container_dir}/{uri[uri.rfind('/')+1:len(uri)].replace(':','_')}.sif"
    if os.path.isfile(sif):
        print(f"Container exists - will not overwrite ({sif})")
        return sif

    cmdArray=["singularity","pull","--dir",container_dir,uri]
    print(cmdArray)
    ret = subprocess.call(cmdArray)
    
    if not os.path.isfile(sif):
        raise Exception(f"Failed to pull container specified. {sif} not found")
        
    return sif

def pull_source_data(project,subject,session):
    wd=aircrush.config['COMPUTE']['working_directory']
    datacommons=aircrush.config['COMPUTE']['commons_path']
    #Test if we are on an HCP node, use sbatch to perform rsync if so

    root=f"/projects/{project.field_path_to_exam_data}/datasets/source"
    

    #If none of the if statements below match, the entire source will be cloned
    print(f"start root:{datacommons}/{root}")
    if os.path.isdir(f"{datacommons}/{root}/{subject.title}"):
        root=f"{root}/{subject.title}"
        if os.path.isdir(f"{datacommons}/{root}/{session.title}"):
            root=f"{datacommons}/{root}/{session.title}"

    if os.path.isdir(f"{datacommons}/{root}/sub-{subject.title}"):
        root=f"{root}/sub-{subject.title}"
        if os.path.isdir(f"{datacommons}/{root}/ses-{session.title}"):
            root=f"{root}/ses-{session.title}"
            print(f"new root:  {root}")
        
    source_session_dir=f"{datacommons}/{root}"
    target_session_dir=f"{wd}/{root}"

    # if os.path.isdir(target_session_dir):
    #     #It's already there, ignore quietly
    #     print(f"{target_session_dir} Already exists")
    #     return
    # else:
    print(f"Cloning ({source_session_dir}) to local working directory ({target_session_dir})")
    os.makedirs(target_session_dir,exist_ok=True)         

    # ret = subprocess.getstatusoutput("which sbatch")
    # if ret[0]==0:
    #     print("sbatch exists, starting asynchronous copy")
    # else:
    #     print("SBATCH doesn't exist, performing synchronous copy")
        
    if not os.path.isdir(source_session_dir):
        raise Exception(f"Subject/session not found on data commons ({source_session_dir})")
    rsync_cmd=f"rsync -r --ignore-missing-args {source_session_dir} {target_session_dir}"
    print(rsync_cmd)
    
    ret = subprocess.getstatusoutput(rsync_cmd)
    if ret[0]!=0:
        raise Exception(f"Failed to copy session directory: {ret[1]}")
        
def pull_data(stage,project,subject,session):
    if stage=="source":
        pull_source_data(project,subject,session)
    else:
        wd=aircrush.config['COMPUTE']['working_directory']

        ##Get the hostname of cluster hosting data commons for remote rsync
        ##user must have setup known_hosts for unattended rsync
        if aircrush.config.has_option('COMMONS','data_transfer_node'):            
            data_transfer_node=aircrush.config['COMMONS']['data_transfer_node']
            if not data_transfer_node=="":                
                # if not data_transfer_node[-1]==":":  #Add a colon to the end for rsync syntax if necessary
                #     data_transfer_node=f"{data_transfer_node}:"
                print(f"{WARNING}The data commons is found on data transfer node {data_transfer_node} User must have setup unattended rsync using ssh-keygen for this process to be scheduled.  If this node is local, remove the data_transfer_node option from crush.ini{ENDC}")
                
        else:
            data_transfer_node=""

        datacommons=aircrush.config['COMMONS']['commons_path']
        if stage =="derivatives":
            #Look on the data commons for any derivative sub-folder containing this subject/session
            
            derivatives=_get_derivatives(data_transfer_node=data_transfer_node,
                                        project=project.field_path_to_exam_data,
                                        datacommons=datacommons,
                                        subject=subject.title,
                                        session=session.title)

            root=f"/projects/{project.field_path_to_exam_data}/datasets/{stage}"
            for derivative in derivatives:
                source=f"{datacommons}/projects/{project.field_path_to_exam_data}/datasets/{stage}/{derivative}"
                target=f"{wd}/projects/{project.field_path_to_exam_data}/datasets/{stage}/{derivative}"                
                _rsync_get(data_transfer_node=data_transfer_node,
                            source=source,                            
                            target=target)
        else:
            source=f"{datacommons}/projects/{project.field_path_to_exam_data}/datasets/{stage}/sub-{subject.title}/ses-{session.title}/"
            target=f"{wd}/projects/{project.field_path_to_exam_data}/datasets/{stage}/sub-{subject.title}/ses-{session.title}/"
            _rsync_get(data_transfer_node=data_transfer_node,
                            source=source,
                            target=target)


def _get_derivatives(**kwargs):
    data_transfer_node=kwargs['data_transfer_node'] if 'data_transfer_node' in kwargs else None 
    project=kwargs['project'] if 'project' in kwargs else None 
    datacommons=kwargs['datacommons'] if 'datacommons' in kwargs else None 
    subject=kwargs['subject'] if 'subject' in kwargs else None
    session=kwargs['session'] if 'session' in kwargs else None

    COMMAND=f"find {datacommons}/projects/{project}/datasets/derivatives -maxdepth 2"
    print(f"ssh {data_transfer_node} {COMMAND}")
    ssh = subprocess.Popen(["ssh", data_transfer_node, COMMAND],
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()

    if result==[]:
        error=ssh.stderr.readlines()    
        raise Exception(f"Failed to find derivatives: {error}")
    else:
        derivatives=result#result.splitlines()

    to_check=[]
    
    if subject is not None and session is not None:
        to_check.append(f"/sub-{subject}/ses-{session}") 
    if subject is not None:
        to_check.append(f"/sub-{subject}")
    to_return=[]
    
    for derivative in derivatives:
        #If the end of this derivative matches the subeject/[session] we are looking
        #for then added it to the to_return list        
        derivative_str=derivative.decode().strip()
        for checkme in to_check:
            if derivative_str[len(derivative_str)-len(checkme):]==checkme:
                to_return.append(derivative_str[len(f"{datacommons}/projects/{project}/datasets/derivatives"):])
    return to_return

def _rsync_get(**kwargs):
    data_transfer_node=kwargs['data_transfer_node'] if 'data_transfer_node' in kwargs else None 
    source=kwargs['source'] if 'source' in kwargs else None
    target=kwargs['target'] if 'target' in kwargs else None

    if source is None or target is None or data_transfer_node is None:
        raise Exception("Insufficient args passed to _rsync_get")
    
    os.makedirs(target,exist_ok=True)     

    if data_transfer_node=="":
        if not os.path.isdir(source):
            raise Exception(f"Subject/session not found on data commons ({source})")
    else:
        source=f"{data_transfer_node}:{source}"

    rsync_cmd=["rsync","-rvvhP","--ignore-missing-args", f"{source}",f"{target}"]      
    print(rsync_cmd)      
    ret,out = getstatusoutput(rsync_cmd)
    if ret!=0:
        raise Exception(f"Failed to copy session directory: {out}")



def getstatusoutput(command):
    print(command)    
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    return (process.returncode, out)

def push_data(stage,project,subject,session,**kwargs):
    print(f"\tPushing up {project.title}/{subject.title}/{session.title} ")
    if stage=="source":
        print("ERROR: Source data is read-only.  It cannot be pushed back to the data commons")
        return
    else:
        wd=aircrush.config['COMPUTE']['working_directory']

        ##Get the hostname of cluster hosting data commons for remote rsync
        ##user must have setup known_hosts for unattended rsync
        if aircrush.config.has_option('COMMONS','data_transfer_node'):         
            data_transfer_node=aircrush.config['COMMONS']['data_transfer_node']
            if not data_transfer_node=="":                
                if not data_transfer_node[-1]==":":  #Add a colon to the end for rsync syntax if necessary
                    data_transfer_node=f"{data_transfer_node}:"
                print(f"{WARNING}The data commons is found on data transfer node {data_transfer_node}. User must have setup unattended rsync using ssh-keygen for this process to be scheduled.  If this node is local, remove the data_transfer_node option from crush.ini.{ENDC}")
                
        else:
            data_transfer_node=""

        datacommons=aircrush.config['COMMONS']['commons_path']
        #Test if we are on an HCP node, use sbatch to perform rsync if so

        if stage=="derivatives":
            if "pipelines" in kwargs:
                pipelines=kwargs['pipelines']
                for pipeline in pipelines:

                    root=f"projects/{project.field_path_to_exam_data}/datasets/{pipeline.field_id}/{stage}/sub-{subject.title}/ses-{session.title}/"
                    source_session_dir=f"{wd}/{root}"
                    target_session_dir=f"{data_transfer_node}{datacommons}/{root}"
                    print(f"Cloning ({source_session_dir}) back to data commons ({target_session_dir})")        
                    print(f"DTN:[{data_transfer_node}]") 
                    rsync_cmd=["rsync","-r","--ignore-missing-args", f"{source_session_dir}",f"{target_session_dir}"]                  
                    ret,out = getstatusoutput(rsync_cmd)
                    if ret!=0:
                        raise Exception(f"Failed to copy session directory: {out}")

            else:
                raise Exception("WARNING: You attepted to return derivatives to the data commons but did not specify which pipelines.")
                

        else:
            root=f"projects/{project.field_path_to_exam_data}/datasets/{stage}/sub-{subject.title}/ses-{session.title}/"
            source_session_dir=f"{wd}/{root}"
            target_session_dir=f"{data_transfer_node}{datacommons}/{root}"
            print(f"Cloning ({source_session_dir}) back to data commons ({target_session_dir})")        
            print(f"DTN:[{data_transfer_node}]") 
            rsync_cmd=["rsync","-r","--ignore-missing-args", f"{source_session_dir}",f"{target_session_dir}"]                  
            ret,out = getstatusoutput(rsync_cmd)
            if ret!=0:
                raise Exception(f"Failed to copy session directory: {out}")
# def test_prereqs(parms,**kwargs):

#     project=kwargs['project'] if 'project' in kwargs else ""       
#     subject=kwargs['subject'] if 'subject' in kwargs else ""
#     session=kwargs['session'] if 'session' in kwargs else ""
#     workingdir=kwargs['workingdir'] if 'workingdir' in kwargs else ""
#     datacommons=kwargs['datacommons'] if 'datacommons' in kwargs else ""
#     pipeline=kwargs['pipeline'] if 'pipeline' in kwargs else ""
    
#     if "prereq-diskspace" in parms:
#         prereq_diskspace=parms["prereq-diskspace"]
#         if aircrush.config.has_option('COMPUTE','available_disk'):
#             diskspace_cmd=aircrush.config['COMPUTE']['available_disk']
#             shell_cmd=[diskspace_cmd]                  
#             ret,out = getstatusoutput(shell_cmd)
#             if ret!=0:
#                 print(f"Failed to assess available diskspace. Attempted:{diskspace_cmd}, Result: {out}")
#                 #raise Exception(f"Failed to assess available diskspace. Attempted:{diskspace_cmd}, Result: {out}")
#                 return False
#             else:
#                 requirement=parse_size(prereq_diskspace)
#                 found=parse_size(out)
#                 if found<requirement:
#                     print(f"[WARNING]: Prerequisite not met: Insufficient disk space to run this operation, {diskspace_cmd} required")
#                     return False

#         else:
#             print(f"[WARNING]: A diskspace prerequisite of {prereq_diskspace} has been specified but the compute node has not been configured to assess available diskspace.  Create an entry in ~/.crush.ini in the COMPUTE section called available_disk with a one line bash command to derive available disk space")
#     return False


        
 
def parameter_expansion(cmdArray,parms_to_add,**kwargs):
    project=None
    subject=None
    session=None
    workingdir=""
    datacommons=""
    pipeline=""    
    if 'project' in kwargs:
        project=kwargs['project']
    if 'subject' in kwargs:
        subject=kwargs['subject']
    if 'session' in kwargs:
        session=kwargs['session']
    if 'workingdir' in kwargs:
        workingdir=kwargs['workingdir']
    if 'datacommons' in kwargs:
        datacommons=kwargs['datacommons']
    if 'pipeline' in kwargs:
        pipeline=kwargs['pipeline']
 

    for k in parms_to_add:   

        parm= parms_to_add[k]
        parm = parm.replace("#workingdir",workingdir)
        parm = parm.replace("#datacommons",datacommons)
        parm = parm.replace("#pipeline",pipeline.field_id)
        parm = parm.replace("#subject",subject.title)
        parm = parm.replace("#session",session.title)
        parm = parm.replace('#project',project.field_path_to_exam_data)
        parm = parm.replace('#datasetdir',f"{workingdir}/projects/{project.field_path_to_exam_data}/datasets/")

        if not k[0:7]=="sbatch-":
            cmdArray.append(f"--{k}")
            cmdArray.append(parm) 
   
    return cmdArray
def ini_settings():
    
    homedir=os.path.expanduser('~')
    crush_config=f"{homedir}/.crush.ini"
    if not os.path.isfile(crush_config):
        
        settings={}
        settings['REST']={}
        settings['COMPUTE']={}
        settings['COMMONS']={}
        print(f"Looks like this is your first time here.  Let's get setup, settings will be stored in ~/.crush.ini")
        
        conf = open(crush_config, "w") 

        settings['REST']['endpoint']=input("What is the URL of your Aircrush CMS [http://20.63.59/9/]:")
        if settings['REST']['endpoint'] == "":
            settings['REST']['endpoint']= "http://20.63.59.9/"

        settings['REST']['username']=input("Aircrush username:")
        while settings['REST']['username']=="":
            settings['REST']['username']=input("Aircrush username:")
        settings['REST']['password']=input("Aircrush password:")
        while settings['REST']['password']=="":
            settings['REST']['password']=input("Aircrush password:")

        hostname=os.environ.get("CC_CLUSTER")
        if hostname==None:
            hostname=socket.gethostname()
        settings['COMPUTE']['cluster']=input(f"Cluster name [{hostname}]")
        if settings['COMPUTE']['cluster']=="":
            settings['COMPUTE']['cluster']=hostname

        settings['COMPUTE']['account']=input("SLURM account to charge (e.g. def-username):")
        while settings['COMPUTE']['account']=="":
            settings['COMPUTE']['account']=input("SLURM account to charge (e.g. def-username):")
        
        scratch=os.environ.get("SCRATCH")
        if scratch==None:
            scratch="~/scratch"
        settings['COMPUTE']['working_directory']=input(f"Working directory for scratch [{scratch}/aircrush]:")
        if settings['COMPUTE']['working_directory']=="":
            settings['COMPUTE']['working_directory']=f"{scratch}/aircrush"
        os.makedirs(settings['COMPUTE']['working_directory'])

        
        settings['COMPUTE']['concurrency_limit']=input("Max concurrent jobs [10]:")
        if settings['COMPUTE']['concurrency_limit']=="":
            settings['COMPUTE']['concurrency_limit']=10

        settings['COMPUTE']['seconds_between_failures']=input("Seconds to wait between failures[18000](default 5 hrs, providing time for mitigation):")
        if settings['COMPUTE']['seconds_between_failures']=="":
            settings['COMPUTE']['seconds_between_failures']=18000
        
        settings['COMMONS']['commons_path']=input(f"Location of data commons.  If DC is remote, provide path on that host.  (e.g. ...[HERE]/projects/project-id/datasets/source):")
        while settings['COMMONS']['commons_path']=="":
            print("\thint: /home/username/projects/def-username/shared/")
            settings['COMMONS']['commons_path']=input(f"Location of data commons (e.g. ...[HERE]/projects/project-id/datasets/source):")

        settings['COMMONS']['data_tranfer_node']=input(f"Data transfer node of cluster hosting data commons (leave blank if the data commons is on this cluster):")
        
        settings['COMPUTE']['singularity_container_location']=input(f"Location for storing active singularity containers [{settings['COMMONS']['commons_path']}/code/containers]:")
        if settings['COMPUTE']['singularity_container_location']=="":
            settings['COMPUTE']['singularity_container_location']=f"{settings['COMMONS']['commons_path']}/code/containers"
        os.makedirs(settings['COMPUTE']['singularity_container_location'])
    
        print("Writing file")
        L = [
            "[REST]\n",
            f"username={settings['REST']['username']}\n",
            f"password={settings['REST']['password']}\n",
            f"endpoint={settings['REST']['endpoint']}\n\n",
            "[COMPUTE]\n",
            f"cluster={settings['COMPUTE']['cluster']}\n",
            f"account={settings['COMPUTE']['account']}\n",
            f"working_directory={settings['COMPUTE']['working_directory']}\n",
            f"concurrency_limit={settings['COMPUTE']['concurrency_limit']}\n",
            f"seconds_between_failures={settings['COMPUTE']['seconds_between_failures']}\n"
            f"singularity_container_location={settings['COMPUTE']['singularity_container_location']}\n\n",
            "[COMMONS]\n",
            f"commons_path={settings['COMMONS']['commons_path']}\n",
            f"data_transfer_node={settings['COMMONS']['data_transfer_node']}\n"
            ]
        conf.writelines(L) 
        conf.close() 
    return AircrushConfig(crush_config)

def createJob(cmdArray,parms_to_add,**kwargs):
    
    taskinstance_uid=kwargs['taskinstance_uid'] if 'taskinstance_uid' in kwargs else None
    project=kwargs['project'].field_path_to_exam_data if 'project' in kwargs else ""        
    subject=kwargs['subject'].title if 'subject' in kwargs else ""        
    session=kwargs['session'].title if 'session' in kwargs else ""
    workingdir=kwargs['workingdir'] if 'workingdir' in kwargs else aircrush.config['COMPUTE']['working_directory']
    datacommons=kwargs['datacommons'] if 'datacommons' in kwargs else aircrush.config['COMPUTE']['commons_path']
    pipeline=kwargs['pipeline'] if 'pipeline' in kwargs else ""

    sbatch_time = parms_to_add['sbatch-time'] if 'sbatch-time' in parms_to_add else ""
    sbatch_account = aircrush.config['COMPUTE']['account']
    sbatch_cpus_per_task = parms_to_add['sbatch-cpus-per-task'] if 'sbatch-cpus-per-task' in parms_to_add else ""
    sbatch_mem_per_cpu = parms_to_add['sbatch-mem-per-cpu'] if 'sbatch-mem-per-cpu' in parms_to_add else ""

    if not os.path.exists(f"{workingdir}/jobs/{project}/{subject}"):
        os.makedirs(f"{workingdir}/jobs/{project}/{subject}")

    attempt=1
    
    basefile=f"{workingdir}/jobs/{project}/{subject}/{session}_{taskinstance_uid}_{attempt}"
    while os.path.isfile(f"{basefile}.sl"):
        attempt+=1
        basefile=f"{workingdir}/jobs/{project}/{subject}/{session}_{taskinstance_uid}_{attempt}"

    jobfile=f"{basefile}.sl"
    stdoutfile=f"{basefile}.out"
    stderrfile=f"{basefile}.err"

    conf = open(jobfile, "w") 
    L = [
        "#!/bin/bash",
        f"#SBATCH -e {stderrfile}",
        f"#SBATCH -o {stdoutfile}",
        f"#SBATCH --time {sbatch_time}" if not sbatch_time=="" else "",
        f"#SBATCH --account {sbatch_account}" if not sbatch_account=="" else "",
        f"#SBATCH --cpus-per-task {sbatch_cpus_per_task}" if not sbatch_cpus_per_task=="" else "",
        f"#SBATCH --mem-per-cpu {sbatch_mem_per_cpu}" if not sbatch_mem_per_cpu=="" else "",
        "module load singularity/3.8",
        ' '.join(cmdArray),
    ]
    job_script = '\n'.join(L)
    conf.write(job_script) 
    conf.close() 
    print(f"Slurm job written to {jobfile}")
    toreturn={}
    toreturn['jobfile']=jobfile
    toreturn['stdout']=stdoutfile
    toreturn['stderr']=stderrfile
    return toreturn

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

    ########### PENDING
# Job ID: 18982311
# Cluster: cedar
# User/Group: dmattie/dmattie
# State: PENDING
# Cores: 1
# Efficiency not available for jobs in the PENDING state.
    return 'COMPLETED'
def check_running_jobs(node_uuid):
    w=Workload(aircrush)
    tis =w.get_running_tasks(node_uuid)
    active_tis=len(tis)
    reviewed_tis=active_tis
    if active_tis>0:
        print(f"Checking for status on {active_tis} jobs thought to be running on this compute node.")
    for ti in tis:        
        if tis[ti].field_jobid:
            #seff_cmd=f"/usr/bin/local/seff {tis[ti].field_jobid}"
            seff_cmd=['seff',f"{tis[ti].field_jobid}"]
            print(seff_cmd)
            try:
                ret = subprocess.run(seff_cmd,   
                                capture_output=True,
                                text=True,                                
                                timeout=60)
    #            ret = subprocess.call(cmdArray)
                print(ret.returncode)
                if ret.returncode==0:
                    status=get_seff_completion_state(ret.stdout)
                    print(f"\t{tis[ti].field_jobid} {status}")
                    if status=='COMPLETED':                        
                        tis[ti].field_seff=ret.stdout
                        if tis[ti].field_logfile and os.path.isfile(tis[ti].field_logfile):
                            logfile = open(tis[ti].field_logfile,'r')

                            log_contents = logfile.read()
                            if len(log_contents)>2000:
                                log_contents=f"log file has been truncated.  see output log for complete detail\n\n{log_contents[-2000:]}"                            
                            tis[ti].body=log_contents

                        updateStatus(tis[ti],"processed")
                    if status=='FAILED':
                        if tis[ti].field_errorfile and os.path.isfile(tis[ti].field_errorfile):
                            logfile = open(tis[ti].field_errorfile,'r')
                            error_contents = logfile.read()
                            if len(error_contents)>2000:
                                error_contents=f"log file has been truncated.  see error log for complete detail\n\n{error_contents[-2000:]}"
                            tis[ti].field_errorlog=error_contents

                        if not tis[ti].field_remaining_retries:
                            tis[ti].field_remaining_retries=5
                            tis[ti].field_seff=ret.stdout
                            updateStatus(tis[ti],"failed")
                        else:
                            if tis[ti]==0:
                                tis[ti].field_seff=ret.stdout
                                updateStatus(tis[ti],"halted","Too many failed retries.  This job will not continue without manual intervention")
                            else:
                                tis[ti].field_remaining_retries-=1
                                tis[ti].field_seff=ret.stdout
                                updateStatus(tis[ti],"failed")                    
                reviewed_tis=reviewed_tis-1
            except Exception as e:
                print(f"{FAIL}[ERROR]{ENDC} Failed to execute seff, {e}")
    if reviewed_tis > 0:
        print(f"\t{reviewed_tis} jobs not accounted for")
    else:
        print("\tAll running jobs on this node accounted for and updated in CMS")

def validate_config():
    passed=True
    
    if not aircrush.config.has_option('REST','username'):
        print("Configuration settings incomplete, expected [REST] username")
        passed=False
    if not aircrush.config.has_option('REST','password'):
        print("Configuration settings incomplete, expected [REST] password")
        passed=False        
    if not aircrush.config.has_option('REST','endpoint'):
        print("Configuration settings incomplete, expected [REST] endpoint")
        passed=False  

    if not aircrush.config.has_option('COMPUTE','cluster'):
        print("Configuration settings incomplete, expected [COMPUTE] cluster")
        passed=False
    if not aircrush.config.has_option('COMPUTE','account'):
        print("Configuration settings incomplete, expected [COMPUTE] account")
        passed=False
    if not aircrush.config.has_option('COMPUTE','working_directory'):
        print("Configuration settings incomplete, expected [COMPUTE] working_directory")
        passed=False        
    if not aircrush.config.has_option('COMPUTE','singularity_container_location'):
        print("Configuration settings incomplete, expected [COMPUTE] singularity_container_location")
        passed=False    

    if not aircrush.config.has_option('COMMONS','commons_path'):
        print("Configuration settings incomplete, expected [COMMONS] commons_path")
        passed=False   
    # if not aircrush.config.has_option('COMMONS','staging_path'):
    #     print("Configuration settings incomplete, expected [COMMONS] staging_path")
    #     passed=False                   
    
    return passed

def doSomething():
    
    #nuid = "4d065840-dd33-44dc-be97-623e7d743bce" #dmattie on narval
    nuid,isready = getMyComputeNodeUUID()

    if not isready:
        print("This worker node is not ready to do more.")
        return    
    
    print(f"{HEADER}Updating sessions allocated to this worker{ENDC}")
    check_running_jobs(nuid)
    cascade_status_to_subject(nuid)

    if args.statusonly:
        return

    
    print(f"{HEADER}Allocating new tasks{ENDC}")
    w=Workload(aircrush) #The list of things to do
    
    todo = w.get_next_task(node_uuid=nuid) #Do whatever the big brain tells us to do next
    if(todo):  #Todo is a TaskInstance    

        workingdir=aircrush.config['COMPUTE']['working_directory']   
        datacommons=aircrush.config['COMMONS']['commons_path']     
        task_counter=1
        task_counter_limit=50
        while (todo): 
            #print("----------Got one-------------")  
            task_col =  TaskCollection(cms_host=crush_host)
            task = task_col.get_one(uuid=todo.field_task)    



            messages=[]
            now = datetime.datetime.now()
                
            try:
                #updateStatus(todo,"running","","")
                messages.append(f"Invoking operator: {task.field_operator} =====================")        
                messages.append(f"Current date and time: {str(now)}")        

                session_col = SessionCollection(cms_host=crush_host)
                session = session_col.get_one(todo.field_associated_participant_ses)
                pipeline = task.pipeline()

                subject=None
                project=None

                if not session == None:
                    subject = session.subject()
                    if not subject == None:
                        project = subject.project()
                        
                if project == None:
                    print(f"ERROR: Assigned session {session.title} is orphaned or the project is unpublished")
                    continue
               
                try:    
                    container = pullContainer(task.field_singularity_container)
                except Exception as e:
                    print(e)
                    return

                bindings=""
                if args.bind:
                    mounts=args.bind.split() 
                    for mount in mounts:            
                        bindings=bindings+f"-B {mount} "
                else:
                    if aircrush.config.has_option('COMPUTE','bindings'):        
                        bindings=aircrush.config['COMPUTE']['bindings']

                        
                cmdArray=["singularity","run","--app",task.field_operator,bindings,container]              
                try:
                    parms = ast.literal_eval(task.field_parameters) 
                except:
                    msg=f"Parameters for task {task.field_operator} are malformed. Expected valid JSON string:{task.field_parameters}"
                    print(msg)
                    
                    sys.exit(1)    


                # ###### Let's check for any prerequisites before we start
                # ###### E.g. if there is a large disk requirement and that has been set as a hint, check if disk is available first
                
                # try:
                #     prereq_res = test_prereqs(parms,
                #         datacommons=datacommons,
                #         workingdir=workingdir,
                #         project=project,
                #         subject=subject,
                #         session=session,
                #         pipeline=pipeline)                 
                # except Exception as e:
                #     print(e)                    
                #     return
                prereq_res = True
                if prereq_res == False:
                        print_statusline(f"{project.title}:{subject.title}/{session.title} Prerequisites not met.  Attempts remaining:{task_counter}/{task_counter_limit}")                           
                        if task_counter>task_counter_limit:
                            return
                        else:
                            task_counter=task_counter+1
                        continue   
                
                print()

                # pullSession(project,subject,session)

                # pulldata
                print(f"Pulling any necessary data for operation") 

                #pull_data("source",project,subject,session)
                pull_data("rawdata",project,subject,session)
                pull_data("derivatives",project,subject,session)
                #            
                # for k in parms:
                #     if parms[k]=="source":
                #         pull_data("source",project,subject,session)
                #     if parms[k]=="rawdata":
                #         pull_data("rawdata",project,subject,session)
                #     if parms[k=="#erivatives":
                #         pull_data("derivatives",project,subject,session)
                print("Performing Parameter Keyword Expansion")
                cmdArray = parameter_expansion(cmdArray,parms,
                    datacommons=datacommons,
                    workingdir=workingdir,
                    project=project,
                    subject=subject,
                    session=session,
                    pipeline=pipeline)  

                                 
                #messages.append(f"cmdArray:{cmdArray}")            
                print("Creating SLURM job")
                jobfiles = createJob(cmdArray,parms,
                    datacommons=datacommons,
                    workingdir=workingdir,
                    project=project,
                    subject=subject,
                    session=session,
                    taskinstance_uid=todo.uuid)    
    
                sbatch_cmd=["sbatch",jobfiles['jobfile']]
                print(sbatch_cmd)
                ret = subprocess.run(sbatch_cmd,                               
                                capture_output=True,
                                text=True,                                
                                timeout=60)      
                                #shell=True,                                       
                                #  stdout=subprocess.PIPE,
                                # stderr=subprocess.PIPE, 
                print("sbatch returned")

                if ret.returncode==0:
                    jobid=get_slurm_id(ret.stdout)
                    print(f"SLURM assigned job id:{jobid}")
                    if jobid!=0:
                        todo.field_jobid=jobid
                        todo.field_seff=""
                        todo.field_errorfile=jobfiles['stderr']
                        todo.field_logfile=jobfiles['stdout']

                        if os.path.isfile(jobfiles['jobfile']):
                            sbatch_contents_handle = open(jobfiles['jobfile'],'r')
                            sbatch_contents = sbatch_contents_handle.read()
                            messages.append(sbatch_contents)

                        updateStatus(todo,"running",'<br/>\n'.join(messages),ret.stderr)
                    else:
                        messages.append(f"\nERROR: SLURM ID returned was unexpectedly 0.")  
                        updateStatus(todo,"failed",'<br/>\n'.join(messages),ret.stderr)
                else:
                    updateStatus(todo,"failed",'<br/>\n'.join(messages),ret.stderr)
            except Exception as e:
                print(e)
                print("An error has accurred.  Unable to proceed.  See previous messages.")
                if hasattr(e, 'message'):
                    new_errors=e.message
                else:
                    new_errors=str(e)
                messages.append(f"Current date and time: {str(now)}")  
                updateStatus(todo,"failed",'<br/>\n'.join(messages),new_errors)
            
            #Get the next thing to do
            #todo = w.get_next_task(node_uuid=nuid) #Do whatever the big brain tells us to do next
            todo=None
            


        # op = invoke_operator(id=todo.uuid,cms_host=crush_host)


            #op_class = getOperatorClassDefinition(todo.field_task) #use the task UUID to determine which operator to use    
            #op = op_class(id=todo.uuid,cms_host=crush_host) #instantiate it based on task instance    
        # op.execute(aircrush)  #Let's do this!
    else:
        print("Nothing to do.")

    # if(todo):  #Todo is a TaskInstance        
    #     print("-----------------------")        
    #     op_class = getOperatorClassDefinition(todo.field_task) #use the task UUID to determine which operator to use    
    #     op = op_class(id=todo.uuid,cms_host=crush_host) #instantiate it based on task instance    
    #     op.execute(aircrush)  #Let's do this!
    # else:
    #     print("Nothing to do.")



#def execute(operator:str,params:str):#**kwargs):

def updateStatus(task_instance,status:str,detail:str="",new_errors:str=""):
    # Valid statuses
    ###########################
    # notstarted,running,failed,completed
    task_instance.field_status=status        
    task_instance.body = detail
    task_instance.field_errorlog = new_errors
    print(f"Updating job status to CMS:{task_instance.field_jobid} ({task_instance.title}")
    uuid=task_instance.upsert()
    

def cascade_status_to_subject(node_uuid):
    node_col=ComputeNodeCollection(cms_host=crush_host);
    node=node_col.get_one(uuid=node_uuid)
    attached_sessions=node.allocated_sessions()
    if aircrush.config.has_option('COMPUTE','concurrency_limit'):
        concurrency_limit = aircrush.config['COMPUTE']['concurrency_limit']
    else:
        concurrency_limit=1000    
    print(f"({len(attached_sessions)}/{concurrency_limit}) sessions allocated to this compute node.")
    
    subjects_of_attached_sessions={}
    for session_uuid in attached_sessions:

        session=attached_sessions[session_uuid]
        count_running=0
        count_failed=0
        count_completed=0
        count_notstarted=0   
        count_processed=0     

        pipelines={}

        ti_col=TaskInstanceCollection(cms_host=crush_host,session=session.uuid)
        tis_for_session=ti_col.get()
        for ti in tis_for_session:
            if tis_for_session[ti].field_status=='completed':
                count_completed+=1
                continue
            if tis_for_session[ti].field_status=='processed':
                count_processed+=1
                continue            
            if tis_for_session[ti].field_status=='running':
                count_running+=1
                continue
            if tis_for_session[ti].field_status=='failed':
                count_failed+=1
                continue
            count_notstarted+=1
            if tis_for_session[ti].field_pipeline:
                pipelines[tis_for_session[ti].field_pipeline]=tis_for_session[ti].pipeline()

        session.field_status=derive_parent_status(count_failed,count_running,count_completed,count_notstarted,count_processed)
        subject=session.subject()                        

        subjects_of_attached_sessions[subject.uuid]=subject
        project=subject.project()        

        if subject == None or project == None:
            print(f"Session {session.title} is orphaned, please conduct a health check.\n\tSubject:{subject}\n\tProject:{project}  Skipping")
            continue
        print(f"Synchronizing {project.title}:{subject.title}/{session.title} with status {session.field_status}")
        if session.field_status=='processed':
            push_data("rawdata",project,subject,session)
            push_data("derivatives",project,subject,session,pipelines=pipelines)
            session.field_status='completed'
            session.field_responsible_compute_node=None #Free up a slot on compute node for more
        session.upsert()

    for subject in subjects_of_attached_sessions:
        count_running=0
        count_failed=0
        count_completed=0
        count_notstarted=0

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
            count_notstarted+=1

        subjects_of_attached_sessions[subject].field_status=derive_parent_status(count_failed,count_running,count_completed,count_notstarted,count_processed)
        subjects_of_attached_sessions[subject].upsert()

def derive_parent_status(failed,running,completed,notstarted,processed):
    if processed > 0 and failed==0 and running==0 and completed==0 and notstarted==0:
        #All session operations are done for this subject, time to push up to data commons
        return "processed"
    if running>0:
        if failed>0:
            return "limping"
        if failed==0:
            return "running"
    
    if failed>0:
        if notstarted==0:
            return "failed"
        else:
            return "limping"

    if completed>0:
        if notstarted==0:
            return "completed"
        else:
            return "running"

    return "notstarted"

def doSync():
    
    dc=DataCommons(aircrush)
    print(f"Syncing with Data Commons {dc.commons_path}")
    dc.initialize()
    dc.SyncWithCMS()

def purge():
    endpoint = aircrush.config['REST']['endpoint']
    yn = input(f"Are you sure you want to delete all task instance, sessions and subjects from {endpoint} ? [N|y]")
    if yn=='y' or yn=='Y':
        print("Purging task instances")
        ti_collection = TaskInstanceCollection(cms_host=crush_host)
        tis = ti_collection.get()
        print(f"\tfound {len(tis)} to delete")
        for ti in tis:
            tis[ti].delete()

       

        print("Purging Subjects")
        sub_collection=SubjectCollection(cms_host=crush_host)
        subjects = sub_collection.get()
        print(f"\tfound {len(subjects)} to delete")
        for sub in subjects:
            subjects[sub].delete()
        
        print("Purging sessions")
        ses_collection = SessionCollection(cms_host=crush_host)
        sessions = ses_collection.get()
        print(f"\tfound {len(sessions)} to delete")
        for ses in sessions:
            sessions[ses].delete()

def main():

    global aircrush,crush_host,args
    print("Welcome to Aircrush!")
    parser = argparse.ArgumentParser(
        description="CRUSH client command line utility. Start all tasks with this command")
    parser.add_argument('-sync',action='store_true',
        help="Synchronize subjects and exams in the data commons with the CMS")

    parser.add_argument('-container',action='store',type=str,
        help="Specify a local container to override whatever the pipeline task intends to use.")
    parser.add_argument('-purge',action='store_true',
        help='Permanently remove all task instances, sessions, and subjects from the CMS' )
    parser.add_argument('-republish',action='store_true',
        help='For any objects in CMS that are unpublished, re-publish them if they probably should be')
    parser.add_argument('-bind',action='store',type=str,
        help='A comma separated list of directories that should be bound to the singularity container so files are accessible to the container')
    parser.add_argument('-statusonly',action='store_true',
        help="Update the status of running jobs, do not invoke more work")
    args = parser.parse_args()

    print("Validating configuration")
    aircrush=ini_settings()
    if not validate_config():
        print ("Configuration incomplete")
        exit(1)


    try:
        crush_host=Host(
            endpoint=aircrush.config['REST']['endpoint'],
            username=aircrush.config['REST']['username'],
            password=aircrush.config['REST']['password']
            )
    except Exception as e:
        print("[ERROR] Unable to connect to CMS.  Check host and settings found in ~/.crush.ini; in section [REST].  Is the CMS accessible?")
        print(e)
        sys.exit(1)

    try:
        os.putenv('AIRCRUSH_CONTAINERS',aircrush.config['COMPUTE']['singularity_container_location'])
    except:
        print("[ERROR] .ini setting now found; expected 'singularity_container_location' in section [COMPUTE]")
        sys.exit(1)

    if (args.sync):
        doSync()
        exit()
    if (args.purge):
        purge()
        exit()
    
    if ready():
        doSomething()

if __name__ == '__main__':
    main()


   