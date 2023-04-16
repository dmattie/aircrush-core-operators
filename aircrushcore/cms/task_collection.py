
from .task import Task
import traceback
import uuid
import asyncio, asyncssh, sys
class TaskCollection():

    def __init__(self,**kwargs):    
         
        self.pipeline=None          

        if "cms_host" in kwargs:
            self.HOST=kwargs['cms_host']
            
        else:
            raise Exception("\nERROR:TaskCollection::CMS host not specified\n")

        if "pipeline" in kwargs:
            self.pipeline=kwargs['pipeline']        
    

    def get_one(self,uuid:str):
        col=self.get(uuid=uuid)        
        if(len(col)>0):
            x = col[list(col)[0]]
            #print(f"xxxx {len(col)}")
            #print(x.title)
            return x
        else:
            return None
            
    def get(self,**kwargs):

        tasks={}   

        if 'uuid' in kwargs:
            uuid=kwargs['uuid']        
            filter_uuid=f"&filter[id][value]={uuid}"
        else:
            filter_uuid=""    

        if self.pipeline!=None:
            filter=f"&filter[field_pipeline.id][value]={self.pipeline}"
        else:
            filter=""

        url=f"jsonapi/node/task?{filter}{filter_uuid}"
            
        r = self.HOST.get(url)
        if r.status_code==200:  #We can connect to CRUSH host           
              
            if len(r.json()['data'])!=0:
                not_done=True
                while not_done:

                    page_of_tasks = self._json2task(r.json()['data'])
                    for tsk in page_of_tasks:
                        tasks[tsk]=page_of_tasks[tsk]                    

                    if 'next' in r.json()['links']:
                        r = self.HOST.get(r.json()['links']['next']['href'])
                    else:
                        not_done=False  
                              
            return tasks
        else:
            return None


    def _json2task(self,json):
         
        tasks={}
        for item in json:
            if(item['type']=='node--task'):

                uuid=item['id']

                prereqs=[]
                for pre in item['relationships']['field_prerequisite_tasks']['data']:
                    prereqs.append(pre)

                metadata={    
                    "title":item['attributes']['title']  ,                            
                    "field_pipeline":item['relationships']['field_pipeline']['data']['id'] ,   
                    "field_id":item['attributes']['field_id'],
                    "field_parameters":item['attributes']['field_parameters'],
                    "field_prerequisite_tasks":prereqs,#item['relationships']['field_prerequisite_tasks']['data']['id'],
                    "field_operator":item['attributes']['field_operator'],
                    "field_singularity_container":item['attributes']['field_singularity_container'],    
                    "field_manual_task":item['attributes']['field_manual_task'],
                    "uuid":uuid,
                    "cms_host":self.HOST                                               
                }                        

                tasks[item['id']]=Task(metadata=metadata)                
        return tasks