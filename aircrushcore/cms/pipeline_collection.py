
from .pipeline import Pipeline
import traceback
import uuid
import asyncio, asyncssh, sys
import logging
class PipelineCollection():

    def __init__(self,**kwargs):    
        
        
        if "cms_host" in kwargs:
            self.HOST=kwargs['cms_host']
            
        else:
            raise Exception("\nERROR:PipelineCollection::CMS host not specified\n")

    def get_one(self,uuid:str):
        col=self.get(uuid=uuid)        
        if(len(col)>0):
            x = col[list(col)[0]]
            return x
        else:
            return None
    def get_one_by_id(self,pipeline_id:str):
        filter_string=f"filter[field_id][value]={pipeline_id}"
        logging.debug(filter_string)
        col=self.get(filter=filter_string)

        if(len(col)>0):
            logging.debug(f"Length of pipeline collection found:{len(col)}")
            p = col[list(col)[0]]
            return p
            
    def get(self,**kwargs):
        pipelines={}    

        if 'uuid' in kwargs:
            uuid=kwargs['uuid']        
            filter_uuid=f"&filter[id][value]={uuid}"
        else:
            filter_uuid=""    
        
        if 'filter' in kwargs:
            filter_arg=kwargs['filter']        
            
        else:
            filter_arg=""

        url=f"jsonapi/node/pipeline?{filter_uuid}{filter_arg}"
        
        r = self.HOST.get(url)
        if r.status_code==200:  #We can connect to CRUSH host           
            
            if len(r.json()['data'])!=0:
                not_done=True
                while not_done:

                    page_of_pipelines = self._json2pipeline(r.json()['data'])
                    for pipe in page_of_pipelines:
                        pipelines[pipe]=page_of_pipelines[pipe]                    

                    if 'next' in r.json()['links']:
                        r = self.HOST.get(r.json()['links']['next']['href'])
                    else:
                        not_done=False

            return pipelines
        else:
            return None

    def _json2pipeline(self,json):
         #for item in r.json()['data']:
        pipelines={}
        for item in json:
            if(item['type']=='node--pipeline'):
            
                uuid=item['id']

                metadata={    
                    "title":item['attributes']['title']  ,                            
                    "field_author":item['attributes']['field_author'],   
                    "field_author_email":item['attributes']['field_author_email'],   
                    "body":item['attributes']['body'],
                    "field_id":item['attributes']['field_id'],
                    "field_plugin_warnings":item['attributes']['field_plugin_warnings'],
                    "uuid":uuid,
                    "cms_host":self.HOST                                               
                }

                pipelines[item['id']]=Pipeline(metadata=metadata)                
        return pipelines