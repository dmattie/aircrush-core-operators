import configparser
from aircrushcore.cms.host import Host
from aircrushcore.cms.subject import Subject
import traceback
import uuid
import asyncio, asyncssh, sys
class SubjectCollection():
    
    
    def __init__(self,**kwargs):    
        
        self.project=None
    

        if "cms_host" in kwargs:
            self.HOST=kwargs['cms_host']
            
        else:
            raise Exception("\nERROR:SubjectRepository::CMS host not specified\n")

        if "project" in kwargs:
            self.project=kwargs['project']
    
    #    self.getKnownParticipants()
    

    def get_one(self,uuid:str):
        col=self.get(uuid=uuid)        
        if(len(col)>0):
            x = col[list(col)[0]]
            return x
        else:
            return None
            
    def get(self,**kwargs):

        subjects={}    
        if 'uuid' in kwargs:
            uuid=kwargs['uuid']        
            filter_uuid=f"&filter[id][value]={uuid}"
        else:
            filter_uuid=""    

        if self.project!=None:
            filter=f"&filter[field_project.id][value]={self.project}"
        else:
            filter=""


        url=f"jsonapi/node/participant?{filter}{filter_uuid}"
        
        r = self.HOST.get(url)
        if r.status_code==200:  #We can connect to CRUSH host           
            
            if len(r.json()['data'])!=0:
                not_done=True
                while not_done:

                    page_of_subjects = self._json2subject(r.json()['data'])
                    for sub in page_of_subjects:
                        subjects[sub]=page_of_subjects[sub]                    

                    if 'next' in r.json()['links']:
                        r = self.HOST.get(r.json()['links']['next']['href'])
                    else:
                        not_done=False

            return subjects
        else:
            return None

    def _json2subject(self,json):
         #for item in r.json()['data']:
        subjects={}
        for item in json:
            if(item['type']=='node--participant'):
                
                uuid=item['id']

                metadata={    
                    "title":item['attributes']['title']  ,                            
                    "field_project":item['relationships']['field_project']['data']['id'] ,   
                    "field_status":item['attributes']['field_status'],
                    "uuid":uuid,
                    "cms_host":self.HOST                                               
                }

                subjects[item['id']]=Subject(metadata=metadata)                
        return subjects

if __name__ == '__main__':
    

    crush_host=Host(
        #endpoint='http://aircrush/',
        endpoint='http://20.63.59.9/',
        username='crush',
        password='crush'
        )
        
    subj_collection=SubjectCollection(cms_host=crush_host)

    s = subj_collection.get()

