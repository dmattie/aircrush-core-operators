import os
from aircrushcore.controller.configuration import AircrushConfig
import socket
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

        settings['REST']['endpoint']=input("What is the URL of your Aircrush CMS [http://141.109.53.209/]:")
        if settings['REST']['endpoint'] == "":
            settings['REST']['endpoint']= "http://141.109.53.209/"

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
    return AircrushConfig(config_file=crush_config)


def validate_config():
    passed=True

    try: aircrush
    except NameError: aircrush=ini_settings()
    
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