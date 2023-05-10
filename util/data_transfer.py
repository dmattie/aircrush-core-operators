import os
import pathlib
import subprocess
import shutil
from .color import ansi
from .config import *
from aircrushcore.cms import Project,Subject,Session
import tarfile
from . import setup
from . import config
from . import sensors

def commit(project:str,subject:str,session:str=""):
    sources=_get_localexam_paths(project,subject,session)
    
    for source in sources:
        #tar files suffixed with ".inprogress" until complete
        tarfile=_tar_dir(sources[source])
        target=_localpath_to_remote(tarfile)     
        target=f"{target}.inprogress"   
        target_parent_dir=os.path.dirname(target)
        _create_remote_path(target_parent_dir)
       
        _ship_tar(tarfile,target)
        ret = _verify_and_remove(tarfile,target)        
        if ret == False:
            raise Exception("Verification of task to commit results to data commons failed.")
        else:
            
            
            shutil.move(tarfile,f"{tarfile}.deleteme")
            print(f"{ansi.OKGREEN}{tarfile}.deleteme has been deleted from working directory{ansi.ENDC}")
            os.remove(f"{tarfile}.deleteme")


def _verify_and_remove(tarfile:str,target:str):
    print("Verification in progress...",end='',flush=True)
    if ":" in target:        
        parts=target.split(":")
        host=parts[0]
        target_filename=parts[1]        
        verify_cmd=["ssh",host,"-o","PasswordAuthentication=no", f"[ -f {target_filename} ]"]     
        print(verify_cmd,flush=True)             
        ret = subprocess.run(verify_cmd,   
            capture_output=True,
            text=True,                             
            timeout=120) #long timeout just in case hanging for a password                              
        if ret.returncode!=0:
            print(f"{ansi.FAIL}Failed{ansi.ENDC} Tar file not found on data commons as expected ({target_filename}) {ret.stderr}",flush=True)
            raise Exception(f"Tar file not found on data commons as expected ({target_filename}) {ret.stderr}")
        else:            
            if target_filename[len(target_filename)-11:]==".inprogress":
                final_name=target_filename[0:len(target_filename)-11]
                move_cmd=["ssh",host,"-o","PasswordAuthentication=no", f"mv {target_filename} {final_name}"] 
                print(move_cmd,flush=True)
                ret = subprocess.run(move_cmd,   
                    capture_output=True,
                    text=True,                             
                    timeout=120) #long timeout just in case hanging for a password                          
                if ret.returncode!=0:
                    print(f"{ansi.FAIL}Failed{ansi.ENDC}")
                    raise Exception(f"Failed to remove .inprogress indicator on target file in data commons ({target_filename})\n{ret.stderr}")
                else:
                    print("done")
                    return True
            else: 
                print("done")              
                return True

    else:        
        if not os.path.isfile(target):
            raise Exception(f"Tar file not found on data commons as expected ({target})")
        else:
            if target[len(target)-11:]==".inprogress":
                final_name=target[0:len(target)-11]  
                print(f"Rename [{target}] => [{final_name}]")   
                shutil.move(target,final_name)
                print("done")
                return True

            print("done!")
            return True
    print(f"{ansi.WARNING}done{ansi.ENDC}")
    return False
        
def _ship_tar(tarfile:str,target:str):
    rsync_cmd=["rsync","-e","ssh -o PasswordAuthentication=no","-r",tarfile,target]      
    print(f"{rsync_cmd}",flush=True)  

    ret = subprocess.run(rsync_cmd,   
                            capture_output=True,
                            text=True)                       
    
    if ret.returncode!=0:
        raise Exception(f"Failed to copy session directory: {ret.stderr}")
    else:
        print(f"Tar file has been copied to data commons\n\tSOURCE:{tarfile}\n\tTARGET:{target}")      
    return True
    
def _localpath_to_remote(path):
    wd=get_config_wd()
    dtn=get_config_dtn()
    dtn_separator=":" if dtn!="" else ""
    datacommons=get_config_datacommons()

    remainder=path[len(wd):]    
    return f"{dtn}{dtn_separator}{datacommons}{remainder}"
def _create_remote_path(path):
    print(f"Creating remote path {path}")
    if ":" in path:
        parts=path.split(":")
        host=parts[0]
        to_create=parts[1]
        #print(f"HOST:{host} path: {to_create}")
        mkdirs_cmd=["ssh",host, f"mkdir -p {to_create}"]                  
        ret = subprocess.run(mkdirs_cmd,   
            capture_output=True,
            text=True,                             
            timeout=60) #long timeout just in case hanging for a password                          
        if ret.returncode!=0:
            raise Exception(f"Failed to copy session directory: {ret.stderr}")
    else:
        os.makedirs(path,exist_ok=True )
        #raise Exception("Malformed path.  Expected host:path")

def _get_localexam_paths(project:str,subject:str,session:str):
    
    paths={}
    wd=get_config_wd()
    #########
    ## RAW
    #########
    
    session_path=f"ses-{session}" if session is not None and session !="" else ""
    session_separator="_" if session_path !="" else ""
    #print(f"[{ansi.WARNING}{session_path}{ansi.ENDC}]")
    #print(f"{wd}/projects/{project}/datasets/sub-{subject}/{session_path}")
    if os.path.isdir(f"{wd}/projects/{project}/datasets/sub-{subject}/{session_path}"):        
        paths['raw']=f"{wd}/projects/{project}/datasets/sub-{subject}/{session_path}"
    elif os.path.isfile(f"{wd}/projects/{project}/datasets/sub-{subject}/sub-{subject}{session_separator}{session_path}.tar"):
        paths['raw']=f"{wd}/projects/{project}/datasets/sub-{subject}/sub-{subject}{session_separator}{session_path}.tar"   
    elif os.path.isdir(f"{wd}/projects/{project}/datasets/sub-{subject}"):
        paths['raw']=f"{wd}/projects/{project}/datasets/sub-{subject}"
    elif os.path.isfile(f"{wd}/projects/{project}/datasets/sub-{subject}.tar"):
        paths['raw']=f"{wd}/projects/{project}/datasets/sub-{subject}.tar"
    ##############
    # DERIVATIVES
    ##############
    local_derivatives=_get_derivatives(dtn="",project=project,subject=subject,session=session)
    counter=1
    for derivative in local_derivatives:
        paths[f"d{counter}"]=f"{wd}/projects/{project}/datasets/derivatives{derivative}"
        counter=counter+1
    return paths
def _tar_dir(dir:str):
    print(f"\nCreating tarfile from source {dir} ...",end='',flush=True)
    print("")
    if not os.path.exists(dir):
        raise Exception(f"{dir} does not exist.")
    if not os.path.isdir(dir):
        if tarfile.is_tarfile(dir):
            print(f"{ansi.WARNING}Already Tarred{ansi.ENDC}({dir})")
            return dir
        else:
            raise Exception(f"{dir} is not a directory but was expected to be.")

    normpath=os.path.normpath(dir)
    lastdir=os.path.basename(normpath)
    secondlastdir=os.path.basename(os.path.dirname(normpath)) 
    if lastdir[0:4]!="sub-" and lastdir[0:4]!="ses-":
        raise Exception(f"{dir} does not appear to be a sub-*/[ses-*] formatted directory") 
    ses_dir=lastdir if lastdir[0:4]=="ses-" else ""
    sub_dir=secondlastdir if secondlastdir[0:4]=="sub-" else lastdir
    if sub_dir[0:4]!="sub-" or (ses_dir[0:4]!="ses-" and ses_dir!=""):
        raise Exception(f"{dir} was intended to be tarred but does not appear to be a sub-/[ses-] format")

    
    separator = "_" if ses_dir[0:4]=="ses-" else ""
    if separator=="":
        cwd_dir=os.path.dirname(normpath)
    else:
        cwd_dir=f"{os.path.dirname(normpath)}"

    # print(f"ses_dir:{ses_dir}")
    # print(f"sub_dir:{sub_dir}")
    # print(f"cwd_dir:{cwd_dir}")
    tar_to_create=f"{cwd_dir}/{sub_dir}{separator}{ses_dir}.tar.inprogress"

    if os.path.isfile(tar_to_create):
        os.remove(tar_to_create)

    tar_cmd=f"tar -cf {tar_to_create} -C {cwd_dir} {lastdir}"
    print(tar_cmd, flush=True)
    ret = subprocess.run(tar_cmd,   
                        capture_output=True,
                        text=True, 
                        shell=True) 
            
    if ret.returncode==0:            
        if os.path.isfile(tar_to_create):
            print(f"Created {ansi.OKBLUE}{tar_to_create}{ansi.ENDC}")
            #print(f"Cleaning up original directory:{cwd_dir}/{lastdir}")
            final_name=tar_to_create[0:len(tar_to_create)-11]
            shutil.move(tar_to_create,f"{final_name}")
            tar_to_create=final_name
            shutil.rmtree(f"{cwd_dir}/{lastdir}")   
            
 
            #print(f"{ansi.OKGREEN}done{ansi.ENDC}")
        else:
            print(f"{ansi.FAIL}FAIL{ansi.ENDC} intended tar to create wasn't created ({tar_to_create}")
            raise Exception(f"Intended tar to create wasn't created ({tar_to_create}")
        
    else:
        print(f"{ansi.FAIL}FAIL{ansi.ENDC} Unable to create tar file for pushing back to data commons\n{ret.stderr}\nCommand Attempted: {tar_cmd}")
        raise Exception(f"Failed creating tar file {ret.stderr}")
    return tar_to_create
def _get_derivatives(dtn:str,project:str,subject:str,session:str):
    if session=="":
        session=None
    data_transfer_node=dtn
    if dtn=="":
        datacommons=get_config_wd()
    else:
        datacommons=get_config_datacommons()

    depth=2 if session is None else 3
    
    COMMAND=f"if [ -d {datacommons}/projects/{project}/datasets/derivatives ];then find {datacommons}/projects/{project}/datasets/derivatives -maxdepth {depth}; fi"
    
    if data_transfer_node=="": #find local derivatives              
        subprocessCmd = subprocess.run([COMMAND],
                        shell=True,
                        capture_output=True,
                        text=True,
                        timeout=600)
        result = subprocessCmd.stdout             
    else:
        print(f"ssh {data_transfer_node} {COMMAND}",flush=True)
        subprocessCmd = subprocess.run(["ssh", data_transfer_node, COMMAND],
                        shell=False,
                        capture_output=True,
                        text=True,
                        timeout=600)
        result = subprocessCmd.stdout

    if subprocessCmd.returncode!=0:
        error=subprocessCmd.stderr  
        raise Exception(f"Failed to find derivatives: {error}")

    if result==[]:
        print("No derivatives found in the data commons")
        return []
        
    else:
        derivatives=result.splitlines()

    to_check=[]
    
    session_path=f"ses-{session}" if session is not None and session !="" else ""
    session_separator="_" if session_path !="" else ""

    if subject is not None and session is not None:
        to_check.append(f"/sub-{subject}/{session_path}") 
        to_check.append(f"/sub-{subject}/sub-{subject}{session_separator}{session_path}.tar")
        to_check.append(f"/sub-{subject}/sub-{subject}{session_separator}{session_path}.tar.gz") 
    elif subject is not None:
        to_check.append(f"/sub-{subject}")
        to_check.append(f"/sub-{subject}.tar")
        to_check.append(f"/sub-{subject}.tar.gz")
    
    to_return=[]
    
    for derivative in derivatives:        
        #If the end of this derivative matches the subeject/[session] we are looking
        #for then added it to the to_return list        
        derivative_str=derivative.strip()
        for checkme in to_check:
            if derivative_str[len(derivative_str)-len(checkme):]==checkme:
                to_return.append(derivative_str[len(f"{datacommons}/projects/{project}/datasets/derivatives"):])
    print(f"\tFound {len(to_return)} derivatives")
    return to_return


def pullContainer(uri:str,container:str=None):
    #return "requirements.txt"   ##TODO
    if (container):
        
        return container
    try: aircrush
    except NameError: aircrush=setup.ini_settings()

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
    # try: aircrush
    # except NameError: aircrush=setup.ini_settings()

    wd=config.get_config_wd()#aircrush.config['COMPUTE']['working_directory']
    datacommons=get_config_datacommons()#aircrush.config['COMPUTE']['commons_path']
    #Test if we are on an HCP node, use sbatch to perform rsync if so

    root=f"/projects/{project.field_path_to_exam_data}/datasets/sourcedata"
    

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
    print(rsync_cmd,flush=True)
    
    ret = subprocess.getstatusoutput(rsync_cmd)
    if ret[0]!=0:
        raise Exception(f"Failed to copy session directory: {ret[1]}")
        
def pull_data(stage,project,subject,session):
    if stage=="source":
        pull_source_data(project,subject,session)
    else:
        #wd=aircrush.config['COMPUTE']['working_directory']
        wd=config.get_config_wd()#aircrush.config['COMPUTE']['working_directory']
        
        ##Get the hostname of cluster hosting data commons for remote rsync
        ##user must have setup known_hosts for unattended rsync
              
        data_transfer_node=config.get_config_dtn()#aircrush.config['COMMONS']['data_transfer_node']
        if not data_transfer_node=="":                
            # if not data_transfer_node[-1]==":":  #Add a colon to the end for rsync syntax if necessary
            #     data_transfer_node=f"{data_transfer_node}:"
            print(f"{ansi.WARNING}The data commons is found on data transfer node {data_transfer_node} User must have setup unattended rsync using ssh-keygen for this process to be scheduled.  If this node is local, remove the data_transfer_node option from crush.ini{ansi.ENDC}")
            
       

        #datacommons=aircrush.config['COMMONS']['commons_path']
        datacommons=get_config_datacommons()#aircrush.config['COMPUTE']['commons_path']        
        if stage =="derivatives":
            #Look on the data commons for any derivative sub-folder containing this subject/session
            
            derivatives=_get_derivatives(dtn=data_transfer_node,
                                        project=project.field_path_to_exam_data,                                       
                                        subject=subject.title,
                                        session=session.title)

            root=f"/projects/{project.field_path_to_exam_data}/datasets/{stage}"
            for derivative in derivatives:
                
                source=f"{datacommons}/projects/{project.field_path_to_exam_data}/datasets/{stage}/{derivative}"
                target=f"{wd}/projects/{project.field_path_to_exam_data}/datasets/{stage}/{derivative}"  
                
                if os.path.isfile(f"{target}.deleteme"):
                    
                    #if there is already a file that was left behind, use that instead
                    #This is used when a project pipeline has been reset at a point in the pipeline using setval
                    #and I don't want to rsync it all back from datacommons when I know the .deleteme is fine to use.
                    #...remove this eventually   TODO
                    
                    print("Extracting local '.deleteme' tarfile...",end='')
                    deleteme_target=f"{target}.deleteme"
                    tar=tarfile.open(deleteme_target)
                    tarpath=pathlib.Path(deleteme_target).parent.resolve()
                    tar.extractall(path=tarpath)
                    tar.close()
                    os.remove(deleteme_target)
                    print(f"{ansi.OKGREEN}local 'DELETEME' version used instead{ansi.ENDC}")

                else:
                    if sensors.exists_on_datacommons("",target):
                        print(f"Local files exist ({target})")
                        continue
                    if sensors.exists_on_datacommons(data_transfer_node,source):                        
                        _rsync_get(data_transfer_node=data_transfer_node,
                                    source=source,                            
                                    target=target)
                    else:
                        print(f"{source} not on datacommons... skipping")
        else:
            foundsource=False
            if stage=="rawdata":
                stage_dir="."
            else:
                stage_dir=stage

            tarsource=f"{datacommons}/projects/{project.field_path_to_exam_data}/datasets/{stage_dir}/sub-{subject.title}/sub-{subject.title}_ses-{session.title}.tar"
            tartarget=f"{wd}/projects/{project.field_path_to_exam_data}/datasets/{stage_dir}/sub-{subject.title}/sub-{subject.title}_ses-{session.title}.tar"
            if sensors.exists_on_datacommons("",f"{wd}/projects/{project.field_path_to_exam_data}/datasets/{stage_dir}/sub-{subject.title}/ses-{session.title}"):
                    print(f"Local files exist ({wd}/projects/{project.field_path_to_exam_data}/datasets/{stage_dir}/sub-{subject.title}/ses-{session.title})")
                    return
            if sensors.exists_on_datacommons(data_transfer_node,tarsource):
                _rsync_get(data_transfer_node=data_transfer_node,
                                source=tarsource,
                                target=tartarget)  
                foundsource=True 
            else:         
                source=f"{datacommons}/projects/{project.field_path_to_exam_data}/datasets/{stage_dir}/sub-{subject.title}/ses-{session.title}/"
                target=f"{wd}/projects/{project.field_path_to_exam_data}/datasets/{stage_dir}/sub-{subject.title}/ses-{session.title}/"
                if sensors.exists_on_datacommons(data_transfer_node,source):
                    _rsync_get(data_transfer_node=data_transfer_node,
                                    source=source,
                                    target=target)
                    foundsource=True    

            if foundsource==False:
                print(f"{source} not on datacommons... skipping")




def _rsync_get(**kwargs):
    data_transfer_node=kwargs['data_transfer_node'] if 'data_transfer_node' in kwargs else None 
    source=kwargs['source'] if 'source' in kwargs else None
    target=kwargs['target'] if 'target' in kwargs else None

    if source is None or target is None or data_transfer_node is None:
        raise Exception("Insufficient args passed to _rsync_get")
    
    target_parent=pathlib.Path(target).parent.resolve()
    os.makedirs(target_parent,exist_ok=True)     

    if data_transfer_node=="":        
        if not os.path.exists(source):
            raise Exception(f"Subject/session not found on data commons ({source})")
        suffix = "/" if os.path.isdir(source) else ""
    else:
        is_dir_cmd=f"ssh {data_transfer_node} test -d source"
        ret = subprocess.run(is_dir_cmd,   
                            capture_output=False,                            
                            shell=True,                               
                            timeout=60)                       
        suffix = "/" if ret.returncode==0 else ""

        source=f"{data_transfer_node}:{source}"

    rsync_cmd=["rsync","-ra","--ignore-missing-args", f"{source}{suffix}",f"{target}"]      
    print(rsync_cmd,flush=True)      
    ret,out = getstatusoutput(rsync_cmd)
    if ret!=0:
        raise Exception(f"Failed to copy session directory: {out}")
    if os.path.isfile(target):
        if tarfile.is_tarfile(target):
            print("Extracting tarfile...",end='')
            tar=tarfile.open(target)
            tarpath=pathlib.Path(target).parent.resolve()
            tar.extractall(path=tarpath)
            tar.close()
            os.remove(target)
            print(f"{ansi.OKGREEN}done{ansi.ENDC}")

def getstatusoutput(command):
    #print(command)    
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    return (process.returncode, out)
