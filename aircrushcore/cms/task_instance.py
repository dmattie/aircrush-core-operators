from aircrushcore.cms.pipeline_collection import PipelineCollection
from .task_collection import TaskCollection
from .task import Task
from .session import Session
from .session_collection import SessionCollection
import json

class TaskInstance():
    
    def __init__(self,**kwargs):
               
        self.title=""  
        self.field_associated_participant_ses=""
        self.field_pipeline=""
        self.body=""
        self.field_remaining_retries=""
        self.field_status=None
        self.field_task=""
        self.uuid=None
        self.HOST=None
        self.published=None
        self.field_errorlog=None
        self.field_jobid=None
        self.field_seff=None
        self.field_errorfile=None
        self.field_logfile=None
        self.field_multiplier_duration=None
        self.field_multiplier_memory=None
        self.field_queue_for_retry=None
        self.sticky=None
    
        if 'metadata' in kwargs:
            m=kwargs['metadata']   
                
            self.title=m['title'] if 'title' in m else None                
            self.field_associated_participant_ses=m['field_associated_participant_ses'] if 'field_associated_participant_ses' in m else None                
            self.field_pipeline=m['field_pipeline'] if 'field_pipeline' in m else None                
            self.body=m['body'] if 'body' in m else None                
            self.errorlog=m['field_errorlog'] if 'field_errorlog' in m else None                
            self.field_remaining_retries=m['field_remaining_retries'] if 'field_remaining_retries' in m else None                
            self.field_status=m['field_status'] if 'field_status' in m else None                
            self.field_task=m['field_task'] if 'field_task' in m else None                
            self.HOST=m['cms_host'] if "cms_host" in m else None                
            self.uuid=m['uuid'] if 'uuid' in m else None                
            self.published=m['published'] if 'published' in m else None  #Published indicator is actually 'status'
            self.field_jobid=m['field_jobid'] if 'field_jobid' in m else None                
            self.field_seff=m['field_seff'] if 'field_seff' in m else None                
            self.field_errorfile=m['field_errorfile'] if 'field_errorfile' in m else None                
            self.field_logfile=m['field_logfile'] if 'field_logfile' in m else None                
            self.field_multiplier_duration = m['field_multiplier_duration'] if 'field_multiplier_duration' in m else None
            self.field_multiplier_memory = m['field_multiplier_memory'] if 'field_multiplier_memory' in m else None
            self.field_queue_for_retry = m['field_queue_for_retry'] if 'field_queue_for_retry' in m else None                    
            self.sticky=m['sticky'] if 'sticky' in m else None
                               
                

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def upsert(self):
            if self.field_status=="failed" and self.field_associated_participant_ses is not None:
                ses=self.associated_session()
                sub=ses.subject()
                proj=sub.project()
                if proj.field_treat_failed_as_terminal==True:
                    self.field_status="terminal"
                    print(f"{WARNING}OVERRIDE{ENDC} Project is set to terminate sessions on failure, no re-attempts.  Status is overridden to {OKCYAN}terminal{ENDC}")
            
            payload = {
                "data" : {
                    "type":"node--task_instance",                                                         
                    "attributes":{
                        "title": self.title,                                                                            
                        "body":{
                            "value":self.body
                        },
                        "field_remaining_retries":self.field_remaining_retries                        
                    },
                     "relationships":{}
                             
                }
            }
             # "relationships":{
                    #     "field_associated_participant_ses":{
                    #         "data":{
                    #             "id":self.field_associated_participant_ses,
                    #             "type":"node--session"
                    #         }
                    #     },
                    #     "field_pipeline":{
                    #         "data":{
                    #             "id":self.field_pipeline,
                    #             "type":"node--pipeline"
                    #         }
                    #     },
                    #     "field_task":{
                    #         "data":{
                    #             "id":self.field_task,
                    #             "type":"node--task"
                    #         }
                    #     }                                                                   
                    # }  
            if not self.sticky == None:
                payload['data']['attributes']['sticky']=self.sticky
            if not self.field_status == None:                
                payload['data']['attributes']['field_status']=self.field_status
            if not self.field_errorlog ==None:
                payload['data']['attributes']['field_errorlog']=self.field_errorlog
            if not self.published == None:
                #status is the published flag
                payload['data']['attributes']['status']=self.published
            if not self.field_multiplier_duration == None:
                payload['data']['attributes']['field_multiplier_duration']=self.field_multiplier_duration
            if not self.field_multiplier_memory == None:
                payload['data']['attributes']['field_multiplier_memory']=self.field_multiplier_memory
            if not self.field_queue_for_retry == None:
                payload['data']['attributes']['field_queue_for_retry']=self.field_queue_for_retry

            if self.field_associated_participant_ses:
                field_associated_participant_ses={
                            "data":{
                                "id":self.field_associated_participant_ses,
                                "type":"node--session"
                            }
                    }
                
           #     payload['data']['relationships']['field_associated_participant_ses']['data']['type']='node-session'
           #     payload['data']['relationships']['field_associated_participant_ses']['data']['id']=self.field_associated_participant_ses
                payload['data']['relationships']['field_associated_participant_ses']=field_associated_participant_ses
            
            if self.field_pipeline:                
                #payload['data']['relationships']['field_pipeline']['data']['type']='node--pipeline'
                #payload['data']['relationships']['field_pipeline']['data']['id']=self.field_pipeline
                field_pipeline={                
                        "data":{
                            "id":self.field_pipeline,
                            "type":"node--pipeline"
                        }                    
                }
                payload['data']['relationships']['field_pipeline']=field_pipeline

            if self.field_task:                
                field_task={
                            "data":{
                                "id":self.field_task,
                                "type":"node--task"
                            }
                        }   
               # payload['data']['relationships']['field_task']['data']['type']='node--task'
               # payload['data']['relationships']['field_task']['data']['id']=self.field_task
                payload['data']['relationships']['field_task']=field_task
            
            
            if self.field_jobid:
                payload['data']['attributes']['field_jobid']=self.field_jobid

            if self.field_seff:
                payload['data']['attributes']['field_seff']=self.field_seff
                    
            if self.field_errorfile:
                payload['data']['attributes']['field_errorfile']=self.field_errorfile

            if self.field_logfile:
                payload['data']['attributes']['field_logfile']=self.field_logfile

            if self.uuid:   #Update existing  
                
                payload['data']['id']=self.uuid                                                                     
                r= self.HOST.patch(f"jsonapi/node/task_instance/{self.uuid}",payload)                
            else:            
                #print(json.dumps(payload))
                r= self.HOST.post("jsonapi/node/task_instance",payload)


            if(r.status_code!=200 and r.status_code!=201):  
                 raise ValueError(f"TaskInstance upsert failed [{self.uuid}/{self.title}] on CMS HOST: {r.status_code}\n\t{r.reason}")                             
            else:    
                self.uuid= r.json()['data']['id']     
                return r.json()['data']['id']          

    def delete(self):
        if self.uuid:          
            r= self.HOST.delete(f"jsonapi/node/task_instance/{self.uuid}")
        else: 
            return False
        
        if(r.status_code!=204):                   
            raise ValueError(f"TaskInstance deletion failed [{self.uuid}]\n\t{r.status_code}\n\t{r.reason}]")

        return True            

    def task_definition(self):
        task = TaskCollection(cms_host=self.HOST).get_one(uuid=self.field_task)
        return task
    def associated_session(self):
        session = SessionCollection(cms_host=self.HOST).get_one(uuid=self.field_associated_participant_ses)            
        return session
    def pipeline(self):
        pipeline = PipelineCollection(cms_host=self.HOST).get_one(uuid=self.field_pipeline)
        return pipeline
    def isManual(self):
        task = TaskCollection(cms_host=self.HOST).get_one(uuid=self.field_task)
        if task.field_manual_task==True:
            return True
        else:
            return False
        