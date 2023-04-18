class Pipeline():
    
    def __init__(self,**kwargs):
               
        self.title=""                
        self.field_author=""
        self.field_author_email=""
        self.body=""
        self.field_id=""
        self.field_plugin_warnings=""
        self.uuid=None
        self.HOST=None

        if 'metadata' in kwargs:
            m=kwargs['metadata']
        if 'title' in m:
            self.title=m['title']
        if 'field_author' in m:
            self.field_author=m['field_author']
        if 'field_author_email' in m:
            self.field_author_email=m['field_author_email']      
        if 'body' in m:
            self.body=m['body']
        if 'field_id' in m:
            self.field_id=m['field_id']
        if 'field_plugin_warnings' in m:
            self.field_plugin_warnings=m['field_plugin_warnings']            
        if 'uuid' in m:
            if m['uuid'] != "":
                self.uuid=m['uuid']
        if "cms_host" in m:
            self.HOST=m['cms_host']      
    def __repr__(self):
        to_return=self.title
        to_return+=f"\n\t{self.field_id}"
        return to_return

    def upsert(self):

            payload = {
                "data" : {
                    "type":"node--pipeline",                                                        
                    "attributes":{
                        "title": self.title,                                                    
                        "field_author": self.field_author,
                        "field_author_email":self.field_author_email,                        
                        "field_id":self.field_id,
                        "body":{
                            "value":self.body,
                        },
                        "field_plugin_warnings":self.field_plugin_warnings
                    }                               
                }
            }
            
            if self.uuid:   #Update existing                  
                payload['data']['id']=self.uuid                                                                  
                r= self.HOST.patch(f"jsonapi/node/pipeline/{self.uuid}",payload)                
            else:            
                r= self.HOST.post("jsonapi/node/pipeline",payload)

            if(r.status_code!=200 and r.status_code!=201):  
                 raise ValueError(f"Pipeline upsert failed [{self.uuid}/{self.title}] on CMS HOST: {r.status_code}\n\t{r.reason}")                             
            else:    
                self.uuid= r.json()['data']['id']     
                return r.json()['data']['id']          

    def delete(self):
        if self.uuid:          
            r= self.HOST.delete(f"jsonapi/node/pipeline/{self.uuid}")
        else: 
            return False
        
        if(r.status_code!=204):                   
            raise ValueError(f"Pipeline deletion failed [{self.uuid}\n\t{r.status_code}\n\t{r.reason}]")

        return True