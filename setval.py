import argparse
from pickle import FALSE
from aircrushcore.cms import compute_node, compute_node_collection, session_collection, task_instance_collection
from aircrushcore.controller.configuration import AircrushConfig
from aircrushcore.dag import Workload
from aircrushcore.cms import *
from aircrushcore.datacommons.data_commons import DataCommons
from util.setup import ini_settings

def main():

    global aircrush,crush_host,args
    print("Aircrush set utility")
    parser = argparse.ArgumentParser(
        description="CRUSH client command line utility. Start all tasks with this command")
    parser.add_argument('-set',action='store',type=str,
        help="Specify a setting to change [ti-status|ti-memory-multiplier].")
    parser.add_argument('-uuid',action='store',
        help='UUID of CMS object' )
    parser.add_argument('-value',action='store',
        help='New value')
    args = parser.parse_args()

    aircrush=ini_settings()

    crush_host=Host(
        endpoint=aircrush.config['REST']['endpoint'],
        username=aircrush.config['REST']['username'],
        password=aircrush.config['REST']['password']
    )

    if args.set == "ti-status":
        set_ti_status(uuid=args.uuid,status=args.value)
    if args.set == "ti-memory-multiplier":
        set_ti_memory_multiplier(uuid=args.uuid,val=args.value)        
    else:
        print("Nothing to do")

def set_ti_status(uuid:str, status:str):
    ti_col=TaskInstanceCollection(cms_host=crush_host)
    if ti_col is not None:
        ti=ti_col.get_one(uuid)        
        print(f"{ti.title}\n\tOld Status:{ti.field_status}\n\tNew Status:{status}")
        ti.field_status=status
        ti.upsert()        
        print("Update Complete")
def set_ti_memory_multiplier(uuid:str, val:str):
    ti_col=TaskInstanceCollection(cms_host=crush_host)
    if ti_col is not None:
        ti=ti_col.get_one(uuid)  
        print(f"{ti.title}\n\tOld Memory Multiplier:{ti.field_multiplier_memory}\n\tNew Memory Multiplier:{val}")      
        ti.field_multiplier_memory=val
        ti.upsert()        
        print("Update Complete")        

if __name__ == '__main__':
    main()

