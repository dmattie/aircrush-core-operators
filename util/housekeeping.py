from aircrushcore.datacommons.data_commons import DataCommons  
from aircrushcore.cms import TaskInstanceCollection,SubjectCollection,SessionCollection,TaskCollection,PipelineCollection
from . import config
from . import setup
import logging

def doSync(project):    
    try: aircrush
    except NameError: aircrush=config.ini_settings()        
    dc=DataCommons(aircrush)    
    print(f"Syncing with Data Commons {dc.commons_path}")
    dc.initialize()
    dc.SyncWithCMS(project=project)

def purge():
    try: aircrush
    except NameError: aircrush=config.ini_settings()

    try: crush_host
    except NameError: crush_host=config.get_cms_host()

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
def render_pipeline(pipeline_to_render):
    try: aircrush
    except NameError: aircrush=config.ini_settings()

    try: crush_host
    except NameError: crush_host=config.get_cms_host()

    pipeline_collection=PipelineCollection(cms_host=crush_host)
    pipeline = pipeline_collection.get_one_by_id(pipeline_to_render)
    print(pipeline)
            
    task_collection=TaskCollection(cms_host=crush_host,pipeline=pipeline.uuid)
    tasks = task_collection.get()
    #logging.debug(tasks)
    to_print_uuids=[]
    for task in tasks:
        to_print_uuids.append(task)
    logging.debug(to_print_uuids)
    cnt=0
    nextidx=0
    while len(to_print_uuids)>0:
        next_task=to_print_uuids[nextidx]
        logging.debug(f"next:{tasks[next_task].title} ({next_task})")
        next_pre_reqs=tasks[next_task].field_prerequisite_tasks

        printable=True
        if len(next_pre_reqs)>0:
            logging.debug(f"has prereqs ({next_pre_reqs})")
            for prereq in next_pre_reqs:
#                        print(type(prereq['id']))
                if prereq['id'] in to_print_uuids:
                    printable=False                            
                    logging.debug(f"{prereq} not printable")
        if printable:
            print(f"\t\t{tasks[next_task].title} ({tasks[next_task].uuid})")
            to_print_uuids.remove(next_task)
            nextidx=0
        else:
            nextidx+=1
        cnt+=1
        logging.debug(f"{cnt} printed")
        if cnt>100:
            break