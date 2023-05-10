# Welcome to AirCRUSH

HPC Client for initiating jobs managed by AirCRUSH.

    usage: start [-h] [-sync] [-container CONTAINER] [-purge] [-republish] [-bind BIND] [-verbose] [-statusonly] [-skiptests] [-nibble] [-project PROJECT]
                [-renderpipeline RENDERPIPELINE] [-x]

    CRUSH client command line utility. Start all tasks with this command

    optional arguments:
    -h, --help            show this help message and exit
    -sync                 Synchronize subjects and exams in the data commons 
                          with the CMS
    -container CONTAINER  Specify a local container to override whatever the 
                          pipeline task intends to use.
    -purge                Permanently remove all task instances, sessions, and 
                          subjects from the CMS
    -republish            For any objects in CMS that are unpublished, 
                          re-publish them if they probably should be
    -bind BIND            A comma separated list of directories that should be 
                          bound to the singularity container so files are 
                          accessible to the container
    -verbose              Increase verbosity of output
    -statusonly           Update the status of running jobs, do not invoke 
                          more work
    -skiptests            Skip pre-req tests e.g. (available disk check, 
                          concurrency limits), etc.
    -nibble               Perform only one task rather than the max 
                          (see ~/.crush.ini [COMPUTE] tasks_per_cycle)
    -project PROJECT      Only perform work related to the specified project, 
                          ignore all other projects
    -renderpipeline RENDERPIPELINE
                          Display a visual representation of a specified 
                          pipeline

## Example task parameters

**Title**: Crush

**Calling Pipeline**: Refer back to a created pipeline

**ID**: Will be used as the task operator, passed as apptainer run --app [ID]

**Parameters**: 

    {input_datasets:'#datasetdir',
    'output_location':'derivatives/tractometrics',
    'subject_label':'#subject',
    'session_label':'#session',
    'num_cores':'32',
    'verbose':None,
    'cleanenv':None,
    'sbatch-cpus-per-task':'32',
    'sbatch-time':'23:00:00',
    'sbatch-mem-per-cpu':'2000M',
    'hint-overlay':'25000'}

**Prerequisite tasks:** Refer to other tasks in the same pipeline that must be completed first

**Operator**: Same as ID for now

**singularity container**:  
    
    e.g. filename (only) or uri like this: library://sylabsed/examples/lolcow
    The path to containers is defined in ~/.crush.ini

## Sample ~/.crush.ini
    #comment
    [REST]
    username=crush
    password=********
    #fourtycore endpoint=http://141.109.53.209/
    endpoint=http://206.12.120.113/

    [COMPUTE]
    cluster=narval
    account=def-dmattie
    working_directory=/scratch/dmattie/datacommons
    concurrency_limit=21
    seconds_between_failures=18000
    singularity_container_location=/home/dmattie/code/containers
    available_disk=bash -c "used=`cat ~/temp_diskusage_report|grep '/scratch ('|cut -d '/' -f2|rev|cut -d' ' -f1 |rev|numfmt --from=iec`;limit=`cat ~/temp_diskusage_report|grep '/scratch ('|cut -d '/' -f3|cut -d ' ' -f1|numfmt --from=iec`;echo $((limit-used))"
    disk_used_cmd=cat ~/temp_diskusage_report|grep '/scratch ('|cut -d '/' -f2|rev|cut -d' ' -f1 |rev|numfmt --from=iec
    disk_quota_cmd=cat ~/temp_diskusage_report|grep '/scratch ('|cut -d '/' -f3|cut -d ' ' -f1|numfmt --from=iec
    tasks_per_cycle=10
    minimum_disk_to_act=500G
    bindings=-B /home -B /project -B /scratch

    [COMMONS]
    commons_path=/home/dmattie/projects/rrg-jlevman/shared/
    data_transfer_node=cedar.computecanada.ca
    staging_path=/scratch/dmattie/datacommons/staging
    