from aircrushcore.datacommons.data_commons import DataCommons  
from aircrushcore.cms import TaskInstanceCollection,SubjectCollection,SessionCollection
from . import config
from . import setup

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
