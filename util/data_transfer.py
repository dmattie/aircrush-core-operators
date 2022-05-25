import os
import pathlib
import subprocess
import shutil
from .color import ansi
from .config import *
from aircrushcore.cms import Project,Subject,Session
import tarfile

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
            print(f"{ansi.OKGREEN}{tarfile}.deleteme can be deleted from working directory{ansi.ENDC}")

def _verify_and_remove(tarfile:str,target:str):
    print("Verification in progress...",end='',flush=True)
    if ":" in target:        
        parts=target.split(":")
        host=parts[0]
        target_filename=parts[1]        
        verify_cmd=["ssh",host,"-o","PasswordAuthentication=no", f"[ -f {target_filename} ]"]     
        print(verify_cmd)             
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
                print(move_cmd)
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
                final_name=target[len(target)-11:]  
                print(f"Rename [{target}] => [{final_name}]")   
                shutil.move(target,final_name)
                print("done")
                return True

            print("done!")
            return True
    print(f"{ansi.WARNING}done{ansi.ENDC}")
    return False
        
def _ship_tar(tarfile:str,target:str):
    rsync_cmd=["rsync","-e","ssh -o PasswordAuthentication=no","-ra",tarfile,target]      
    print(f"{rsync_cmd}")  

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
    #print(f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}/{session_path}")
    if os.path.isdir(f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}/{session_path}"):        
        paths['raw']=f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}/{session_path}"
    elif os.path.isfile(f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}/sub-{subject}{session_separator}{session_path}.tar"):
        paths['raw']=f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}/sub-{subject}{session_separator}{session_path}.tar"   
    elif os.path.isdir(f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}"):
        paths['raw']=f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}"
    elif os.path.isfile(f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}.tar"):
        paths['raw']=f"{wd}/projects/{project}/datasets/rawdata/sub-{subject}.tar"
    ##############
    # DERIVATIVES
    ##############
    local_derivatives=_get_derivatives(dtn="",project=project,subject=subject,session=session)
    counter=1
    for derivative in local_derivatives:
        paths[f"d{counter}"]=f"{wd}/projects/{project}/datasets/derivatives{derivative}"
        counter=counter+1
    return paths

def _rsync_put(**kwargs):
    data_transfer_node=kwargs['data_transfer_node'] if 'data_transfer_node' in kwargs else None 
    source=kwargs['source'] if 'source' in kwargs else None
    target=kwargs['target'] if 'target' in kwargs else None

    if source is None or target is None or data_transfer_node is None:
        raise Exception("Insufficient args passed to _rsync_get")
    
    if os.path.isdir(source):        
        tarfile=_tar_dir(source)

        #ensure target variable is a tar:
        normpath_target=os.path.normpath(target)
        ses_target=os.path.basename(normpath_target)
        sub_target=os.path.basename(os.path.dirname(normpath_target))        
        root=root=os.path.dirname(os.path.dirname(normpath_target))

        sespart_target=f"_{ses_target}" if ses_target[0:4]=="ses-" else ""       
        if sub_target[0:4]=="sub-":
            target=f"{root}/{sub_target}/{sub_target}{sespart_target}.tar"
        else:
            raise Exception(f"Target specified to store on datacommons doesn't appear to be BIDS compliant. Expected sub-##.  ({target}) (subject={sub_target})")

    else:
        source=source.rstrip('/')


    # Ensure parent directories exist on target
    if data_transfer_node=="":
        target_parent = pathlib.Path(target).parent.resolve()
        os.makedirs(target_parent,exist_ok=True)   
        #dir_path = os.path.dirname(os.path.realpath(__file__))
        if not os.path.exists(target_parent):
            raise Exception(f"Subject/session PATH not found on data commons ({target_parent})")
    else:   
        target_parent=target[0:target.rindex("/")]
        target=f"{data_transfer_node}:{target}"
        target=target.rstrip('/')
        
        #f"{root}/{sub_target}/{sub_target}"
        print(f"Creating remote directory if missing: {target_parent}")
        mkdirs_cmd=["ssh",data_transfer_node, f"mkdir -p {target_parent}"]                  
        ret,out = getstatusoutput(mkdirs_cmd)
        if ret!=0:
            raise Exception(f"Failed to create target directory:({target}).  Received: {out}")

        
    
    rsync_cmd=["rsync","-ra","--ignore-missing-args", f"{source}",f"{target_parent}"]      
    print(f"{rsync_cmd}")  

    ret = subprocess.run(rsync_cmd,   
                            capture_output=True,
                            text=True,                             
                            timeout=7200) #long timeout just in case hanging for a password                          
    
    if ret.returncode!=0:
        raise Exception(f"Failed to copy session directory: {ret.stderr}")
    else:
        if os.path.isdir(source) and len(sespart_target)>0:
            cleanup_cmd=f"if [ -d {target_parent}/{ses} ] && [ -f {target} ]];then rm -r {target_parent}/{ses} {target_parent}/{ses}; fi"
            if data_transfer_node!="":
                cleanup_cmd=f"ssh {data_transfer_node} '{cleanup_cmd}'"
            
            ret = subprocess.run(cleanup_cmd,   
                            capture_output=True,
                            text=True, 
                            shell=True,
                            timeout=3600) #long timeout just in case hanging for a password              
            if ret.returncode!=0:  
                print(f"{ansi.WARNING}ERROR{ansi.ENDC} cleaning up old directory.  Now both a session directory and a tar exist.  The tar is newer.\n{ret.stderr}\nCommand Attempted:{cleanup_cmd}")
        else:
            print(f"No session directory to cleanup for this subject at target {len(target)}")    
        #######################
        ## DELETE LOCAL COPY ##
        #######################
        shutil.move(source,f"{source}.deleteme")
        print(f"{ansi.WARNING}Safe to remove {normpath}/{ses}{ansi.ENDC}")

    return True

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
        print(f"ssh {data_transfer_node} {COMMAND}")
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