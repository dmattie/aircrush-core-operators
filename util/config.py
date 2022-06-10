from .setup import ini_settings
from .color import ansi
import os
from aircrushcore.cms import Host

def get_cms_host():
    try: aircrush
    except NameError: aircrush=ini_settings()
    cms_host=Host(
            endpoint=aircrush.config['REST']['endpoint'],
            username=aircrush.config['REST']['username'],
            password=aircrush.config['REST']['password']
            )
    return cms_host

def get_config_dtn():
    try: aircrush
    except NameError: aircrush=ini_settings()
     ##Get the hostname of cluster hosting data commons for remote rsync
    ##user must have setup known_hosts for unattended rsync
    if aircrush.config.has_option('COMMONS','data_transfer_node'):         
        data_transfer_node=aircrush.config['COMMONS']['data_transfer_node']
        if not data_transfer_node=="":                
            # if not data_transfer_node[-1]==":":  #Add a colon to the end for rsync syntax if necessary
            #     data_transfer_node=f"{data_transfer_node}:"
            pass
            # print(f"{ansi.WARNING}The data commons is found on data transfer node {data_transfer_node}. User must have setup unattended rsync using ssh-keygen for this process to be scheduled.  If this node is local, remove the data_transfer_node option from crush.ini.{ansi.ENDC}")
            
    else:
        data_transfer_node=""
    return data_transfer_node
def get_config_datacommons():
    try: aircrush
    except NameError: aircrush=ini_settings()
    if aircrush.config.has_option('COMMONS','commons_path'):               
        commons=aircrush.config['COMMONS']['commons_path']
        if commons=="":                            
            raise Exception("The data commons is not defined in ~/.crush.ini.  Set COMMONS/commons_path.")
    else:        
        raise Exception("The data commons is not defined in ~/.crush.ini.  Set COMMONS/commons_path.")
    return commons
def get_config_wd():
    try: aircrush
    except NameError: aircrush=ini_settings()
    if aircrush.config.has_option('COMPUTE','working_directory'):               
        wd=aircrush.config['COMPUTE']['working_directory']
        if wd=="":                            
            raise Exception("The working directory is not defined in ~/.crush.ini.  Set COMPUTE/working_directory.")
    else:        
        raise Exception("The working directory is not defined in ~/.crush.ini.  Set COMPUTE/working_directory.")
    # if wd[0:1]=="~":
    #     wd=os.path.expanduser(wd)
    return os.path.expanduser(wd).rstrip('/')

def get_tasks_per_cycle():   
    try: aircrush
    except NameError: aircrush=ini_settings()         
    if aircrush.config.has_option('COMPUTE','tasks_per_cycle'):
        tpc=aircrush.config['COMPUTE','tasks_per_cycle']
        if tpc.isdigit():
            return int(tpc)
        else:
            raise Exception("The tasks per cycle setting is not an integer.  See ~/.crush.ini COMPUTE/tasks_per_cycle")
    else:
        return 1    