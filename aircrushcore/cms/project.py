import json
class Project():
   
   
    def __init__(self,**kwargs):
        self.title=""                
        self.field_connection_type=""
        self.field_host=""
        self.field_password=""
        self.field_path_to_crush_agent=""
        self.field_path_to_exam_data=""
        self.field_username=""
        self.body=""
        self.uuid=""             
        self.field_activated_pipelines=""
        self.field_treat_failed_as_terminal=""
        self.HOST=""

        if 'metadata' in kwargs:
            m=kwargs['metadata']

        if 'title' in m:
            self.title=m['title']
        if 'field_connection_type' in m:
            self.field_connection_type=m['field_connection_type']
        if 'field_host' in m:
            self.field_host=m['field_host']      
        if 'field_password' in m:
            self.field_password=m['field_password']
        if 'field_path_to_crush_agent' in m:            
            self.field_path_to_crush_agent=m['field_path_to_crush_agent']
        if 'field_path_to_crush_agent' in m:
            self.field_path_to_exam_data=m['field_path_to_exam_data']
        if 'field_path_to_exam_data' in m:
            self.field_path_to_crush_agent=m['field_path_to_crush_agent']
        if 'field_username' in m:            
            self.field_username=m['field_username']                     
        if 'body' in m:
            self.body=m['body']     
        if "uuid" in m:
            self.uuid=m['uuid']        
        if "field_activated_pipelines" in m: #Tuple of UUIDs
            self.field_activated_pipelines=m['field_activated_pipelines']   
        if "field_treat_failed_as_terminal" in m:
            self.field_treat_failed_as_terminal=m['field_treat_failed_as_terminal']
        if "cms_host" in m:
            self.HOST=m['cms_host'] 
   
    def upsert(self):

            payload = {
                "data" : {
                    "type":"node--project",    
                  #  "id":self.uuid,#self.Participants[participant].uuid,                
                    "attributes":{
                        "title": self.title,                                                    
                        "field_connection_type": self.field_connection_type,
                        "field_host":self.field_host,
                        "field_password":self.field_password,
                        "field_path_to_crush_agent":self.field_path_to_crush_agent,
                        "field_path_to_exam_data":self.field_path_to_exam_data,
                        "field_username":self.field_username,
                        "field_treat_failed_as_terminal":self.field_treat_failed_as_terminal,
                        "body":self.body                        
                    },
                    "relationships":{}
                    
                    # "relationships":{
                    #     "field_activated_pipelines":{
                    #         "data":{
                    #             "type":"node--pipeline",
                    #             "id":self.field_activated_pipelines
                    #         }
                    #     }                         
                    # }              
                }
            }
            if self.field_activated_pipelines:
                data=[]
                for pipeline in self.field_activated_pipelines:
                    data.append({
                       "id":pipeline,
                       "type":"node--pipeline"
                    })
                relationship_data={"data":data}
                payload['data']['relationships']['field_activated_pipelines']=relationship_data
                          
            #print(json.dumps(payload))

            if self.uuid:   #Update existing    
                payload['data']['id']=self.uuid                                                
                r= self.HOST.patch(f"jsonapi/node/project/{self.uuid}",payload)
            else:
                r= self.HOST.post("jsonapi/node/project",payload)

            if(r.status_code!=200 and r.status_code!=201):                   
                print(f"[ERROR] failed to upsert project {self.uuid} ({self.title}) on CMS HOST: {r.status_code},  {r.reason}\n\n{payload}")
            else:   
                self.uuid =r.json()['data']['id']      
                return r.json()['data']['id']          
                # if len(r.json()['data'])==0:
                #     print("SubjectCollection::upsert:  subject not updated.")                
                # else:       
                #     return r.json()['data']['id']

    def delete(self):
        if self.uuid:          
            r= self.HOST.delete(f"jsonapi/node/project/{self.uuid}")
        else: 
            return False
        
        if(r.status_code!=204):                   
            raise ValueError(f"Project deletion failed [{self.uuid}]")

        return True

    def get_overrides(self,task_uuid:str):
        overrides={}
        if task_uuid is None:
            print(f"{FAIL}SKIPPED{ENDC} Overrides for project sought, but task operation not specified")
            return overrides
        url=f"jsonapi/node/project/{self.uuid}?include=field_overrides"     
        r = self.HOST.get(url)
        if r.status_code==200:  #We can connect to CRUSH host   
            if 'included' in  r.json():                     
                if len(r.json()['included'])!=0:                    
                    for item in r.json()['included']:                                         
                        if(item['type']=='paragraph--override'):
                            if(item['relationships']['field_task']['data']['id'])==task_uuid:                            
                                name=item['attributes']['field_parameter']
                                value=item['attributes']['field_value']
                                overrides[name]=value
                            
        return overrides        
