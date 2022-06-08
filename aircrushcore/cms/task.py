from .pipeline_collection import PipelineCollection
class Task():
    
    def __init__(self,**kwargs):
               
        self.title=""  
        self.field_pipeline=""
        self.field_id=""
        self.field_parameters=""
        self.field_prerequisite_tasks=""
        self.field_operator=""
        self.field_singularity_container=""
        self.field_manual_task=False
        self.uuid=None
        self.HOST=None
    
        if 'metadata' in kwargs:
            m=kwargs['metadata']   
            if 'title' in m:
                self.title=m['title']        
            if 'field_pipeline' in m:
                self.field_pipeline=m['field_pipeline']
            if 'field_id' in m:
                self.field_id=m['field_id']
            if 'field_parameters' in m:
                self.field_parameters=m['field_parameters']
            if 'field_prerequisite_tasks' in m:
                self.field_prerequisite_tasks=m['field_prerequisite_tasks']
            if 'field_operator' in m:
                self.field_operator=m['field_operator']
            if 'field_singularity_container' in m:
                self.field_singularity_container=m['field_singularity_container']
            if 'field_manual_task' in m:
                self.field_manual_task=m['field_manual_task']
            if "cms_host" in m:
                self.HOST=m['cms_host']    
            if 'uuid' in m:
                self.uuid=m['uuid']


    def upsert(self):

            payload = {
                "data" : {
                    "type":"node--task",                                                         
                    "attributes":{
                        "title": self.title,                                                                            
                        "field_id":self.field_id,
                        "field_operator":self.field_operator,
                        "field_parameters":self.field_parameters
                    },
                    "relationships":{
                        "field_prerequisite_tasks":{
                            "data":[]          
                        },   
                        "field_pipeline":{
                            "data":{
                                "id":self.field_pipeline,
                                "type":"node--pipeline"
                            }
                        }                      
                    }              
                }
            }
            for prereq in self.field_prerequisite_tasks:                
                prereq_obj = {
                    "type":"node--task",
                    "id":self.prereq.uuid
                    }
                payload.data.relationships.field_prerequisite_tasks.append(prereq_obj)
            
            if not self.field_singularity_container == None:
                payload['data']['attributes']['field_singularity_container']=self.field_singularity_container

            if self.uuid:   #Update existing                  
                payload.data.id=self.uuid                                                                  
                r= self.HOST.patch(f"jsonapi/node/task/{self.uuid}",payload)                                
            else:            
                r= self.HOST.post("jsonapi/node/task",payload)

            if(r.status_code!=200 and r.status_code!=201):  
                 raise ValueError(f"Task upsert failed [{self.uuid}/{self.title}] on CMS HOST: {r.status_code}\n\t{r.reason}")                             
            else:    
                self.uuid= r.json()['data']['id']     
                return r.json()['data']['id']          

    def delete(self):
        if self.uuid:          
            r= self.HOST.delete(f"jsonapi/node/task/{self.uuid}")
        else: 
            return False
        
        if(r.status_code!=204):                   
            raise ValueError(f"Task deletion failed [{self.uuid}]\n\t{r.status_code}\n\t{r.reason}]")

        return True            
    
    def pipeline(self):
        pipeline = PipelineCollection(cms_host=self.HOST).get_one(uuid=self.field_pipeline)
        return pipeline

