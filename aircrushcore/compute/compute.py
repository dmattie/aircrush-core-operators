import asyncio, asyncssh, sys
import traceback
import configparser
from aircrushcore.controller.configuration import AircrushConfig
from aircrushcore.compute.compute_node_connection import ComputeNodeConnection
import uuid
import re

class Compute():  

    def __init__(self,conn:ComputeNodeConnection):
        
        self.connection = conn

        crush_config='crush.ini'
        aircrush=AircrushConfig(crush_config)
        self.sbatch_submitted_regex=aircrush.config['REGEX']['SBatchSubmitted']
        print(self.sbatch_submitted_regex)


    def invoke_blocking(self, container:str,mode:str,command:str):
        response={}   
        script=f"""UUID=$(cat /proc/sys/kernel/random/uuid);
singularity pull image_$UUID.sif {container};
singularity {mode} image_$UUID.sif {command};
rm image_$UUID.sif"""
        asyncio.get_event_loop().run_until_complete(
                self._run_ssh_client(
                    host=self.connection.hostname,
                    username=self.connection.username,
                    password=self.connection.password,                
                    cmd=script                 
                )

         
        )  
               
        response['env']=self.agentresult.env
        response['command']=self.agentresult.command
        response['subsystem']=self.agentresult.subsystem
        response['exit_status']=self.agentresult.exit_status
        response['exit_signal']=self.agentresult.exit_signal
        response['returncode']=self.agentresult.returncode
        response['stdout']=self.agentresult.stdout
        response['stderr']=self.agentresult.stderr        
        return response    
                                    
    def invoke_nonblocking(self, container:str,mode:str,command:str,account:str,**kwargs):
        response={}  
        uid=uuid.uuid4()
        sbatch_time=""
        sbatch_mem_per_cpu=""
        sbatch_cpus_per_task=""
        sbatch_nodes=""
        working_directory="~~~"
        rx = re.compile(self.sbatch_submitted_regex,re.MULTILINE)
        
        if 'time' in kwargs:
            sbatch_time=f"#SBATCH --time={kwargs['time']}"
        if 'mem_per_cpu' in kwargs:
            sbatch_mem_per_cpu=f"#SBATCH --mem-per-cpu={kwargs['mem_per_cpu']}"
        if 'cpus_per_task' in kwargs:
            sbatch_cpus_per_task=f"#SBATCH --cpus-per-task={kwargs['cpus_per_task']}"
        if 'nodes' in kwargs:
            sbatch_nodes=f"#SBATCH --nodes={kwargs['nodes']}"
        if 'working_directory' in kwargs:
            working_directory=f"cd {kwargs['working_directory']}"

        shout=f"{uid}.sh"
        
        

        new_command=f"""
mkdir -p {kwargs['working_directory']};
{working_directory};
mkdir -p {uid};
cd {uid};pwd
echo '#!/bin/bash' > {shout};
echo '#SBATCH --account={account}' >> {shout};
echo '#SBATCH -e {uid}.err' >> {shout};
echo '#SBATCH -o {uid}.out' >> {shout};
echo '{sbatch_time}' >> {shout};
echo '{sbatch_mem_per_cpu}' >> {shout};
echo '{sbatch_cpus_per_task}' >> {shout};
echo '{sbatch_nodes}' >> {shout};
echo '{working_directory}' >> {shout};
echo 'mkdir -p {uid}' >> {shout};
echo 'cd {uid}' >> {shout};
echo 'singularity pull image_{uid}.sif {container}' >> {shout};
echo 'singularity {mode} image_{uid}.sif {command}' >> {shout};
sbatch {shout}
"""


        asyncio.get_event_loop().run_until_complete(
                self._run_ssh_client(
                    host=self.connection.hostname,
                    username=self.connection.username,
                    password=self.connection.password,                
                    cmd=new_command                 
                )

         
        )  
               
        response['env']=self.agentresult.env
        response['command']=self.agentresult.command
        response['subsystem']=self.agentresult.subsystem
        response['exit_status']=self.agentresult.exit_status
        response['exit_signal']=self.agentresult.exit_signal
        response['returncode']=self.agentresult.returncode
        response['stdout']=self.agentresult.stdout
        response['stderr']=self.agentresult.stderr

        guid=None
        jobid=None


        for match in rx.finditer(response['stdout']):        
            if len(match.groups()) == 2:
                #we found guid and job id            
                guid = match.groups()[0]
                jobid= match.groups()[1]
                print(f"jobid:{jobid}, guid:{guid}")

        response['job_guid'] = guid
        response['job_id'] = jobid

        return response    
                                    

    async def _run_ssh_client(self,host:str,username:str,password:str,cmd:str):
        #print(f"host={host}, username={username}, password={password}, cmd={cmd}")

        async with asyncssh.connect(host=host,username=username, password=password, known_hosts=None) as conn:            
            self.agentresult = await conn.run(cmd, check=True)
    

        
# (crush2021) [dmattie@cedar1 jobs]$ sbatch test.sh
# Submitted batch job 12759313
# (crush2021) [dmattie@cedar1 jobs]$ squeue -u dmattie
#           JOBID     USER      ACCOUNT           NAME  ST  TIME_LEFT NODES CPUS TRES_PER_N MIN_MEM NODELIST (REASON) 
#        12759313  dmattie def-dmattie_        test.sh  PD       1:00     1    1        N/A    256M  (Priority) 
                                
# (crush2021) [dmattie@cedar1 jobs]$ squeue -j 12759313 
# slurm_load_jobs error: Invalid job id specified
