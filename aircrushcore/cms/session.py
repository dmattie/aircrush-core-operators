#from aircrushcore.cms import *
from .subject_collection import SubjectCollection
from .project_collection import ProjectCollection

class Session():
    
    def __init__(self,**kwargs):
               
        self.title=""                
        self.field_participant=""
        self.field_status=None
        self.uuid=None
        self.HOST=None
        self.published=None
        self.sticky=None
        self.field_responsible_compute_node=""

        if 'metadata' in kwargs:
            m=kwargs['metadata']
        #--------------------------------
        if 'title' in m:
            self.title=m['title']
        if 'field_participant' in m:
            self.field_participant=m['field_participant']
        if 'field_status' in m:
            self.field_status=m['field_status']      
        if 'uuid' in m:
            if m['uuid'] != "":
                self.uuid=m['uuid']
        if "cms_host" in m:
            self.HOST=m['cms_host']    
        if "published" in m:
            self.published=m['published']   
        if "sticky" in m:
            self.sticky=m['sticky']
        if "field_responsible_compute_node" in m:            
            self.field_responsible_compute_node=m['field_responsible_compute_node']

    def upsert(self):

            payload = {
                "data" : {
                    "type":"node--session",                                                        
                    "attributes":{
                        "title": self.title                                                                                                 
                    },
                    "relationships":{}
                    # "relationships":{
                    #     "field_participant":{
                    #         "data":{
                    #             "type":"node--participant",
                    #             "id":self.field_participant
                    #         }
                    #     }                         
                    # }              
                }
            }   
            if not self.field_status == None:
                payload['data']['attributes']['field_status']=self.field_status
            if not self.published == None:
                #status is the published flag
                payload['data']['attributes']['status']=self.published
            if not self.sticky == None:
                payload['data']['attributes']['sticky']=self.sticky
            if self.field_participant:
                field_participant={
                    "data":{
                        "id":self.field_participant,
                        "type":"node--participant"
                    }
                }
                payload['data']['relationships']['field_participant']=field_participant
                                
            if self.field_responsible_compute_node:
                field_responsible_compute_node={
                    "data":{
                        "id":self.field_responsible_compute_node,
                        "type":"node--compute_node"
                    }
                }
                payload['data']['relationships']['field_responsible_compute_node']=field_responsible_compute_node
            else:
                payload['data']['relationships']['field_responsible_compute_node']=None
            
            if self.uuid:   #Update existing                               
                payload['data']['id']=self.uuid                                                                  
                r= self.HOST.patch(f"jsonapi/node/session/{self.uuid}",payload)                
            else:            
                r= self.HOST.post("jsonapi/node/session",payload)                                 
            if(r.status_code!=200 and r.status_code!=201):  
                raise ValueError(f"Session upsert failed [{self.uuid}/{self.title}] on CMS HOST: {r.status_code}\n\t{r.reason}")                             
            else:    
                self.uuid= r.json()['data']['id']     
                return r.json()['data']['id']          

    def delete(self):
        if self.uuid:          
            r= self.HOST.delete(f"jsonapi/node/session/{self.uuid}")
        else: 
            return False
        
        if(r.status_code!=204):                   
            raise ValueError(f"Session deletion failed [{self.uuid}]")

        return True

    def subject(self):
        subject = SubjectCollection(cms_host=self.HOST).get_one(uuid=self.field_participant)
        return subject

    def project(self):
        project = ProjectCollection(cms_host=self.HOST).get_one(uuid=self.subject().field_project)
        return project

    def active_pipelines(self):
            
        subj=self.subject()
        proj=self.project()
        
        return proj.field_activated_pipelines

