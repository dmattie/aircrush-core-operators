
from .host import Host
from .project import Project


class ProjectCollection():
    def __init__(self,cms_host:Host):
        self.HOST = cms_host
        #self.Projects={} 

    def get_one(self,uuid:str):
        col=self.get(uuid=uuid)
        if(len(col)>0):
            p = col[list(col)[0]]
            return p
        return 
    def get_one_by_name(self,project_name:str):
        col=self.get(filter=f"filter[title][value]={project_name}")
        if(len(col)>0):
            p = col[list(col)[0]]
            return p
        
    def get(self,**kwargs):
        Projects={}

        if 'uuid' in kwargs:
            uuid=kwargs['uuid']        
            filter=f"filter[id][value]={uuid}"
        else:
            filter=""

        if 'filter' in kwargs:
            filter_arg=kwargs['filter']        
            
        else:
            filter_arg=""
                        

        url=f"jsonapi/node/project?{filter}{filter_arg}"     
        #print(url)   

        r = self.HOST.get(url)
        if r.status_code==200:  #We can connect to CRUSH host           
              
            if len(r.json()['data'])!=0:
                not_done=True
                while not_done:

                    page_of_projects = self._json2project(r.json()['data'])
                    for proj in page_of_projects:
                        Projects[proj]=page_of_projects[proj]                    
                    if 'next' in r.json()['links']:
                        r = self.HOST.get(r.json()['links']['next']['href'])
                    else:
                        not_done=False
                
            return Projects                     


    def _json2project(self,json):
         #for item in r.json()['data']:
        projects={}
        for item in json:
            if(item['type']=='node--project'):

                uuid=item['id']

                activepipelines=[]

                for ap in item['relationships']['field_activated_pipelines']['data']:                            
                    if ap['type']=='node--pipeline':                                
                        activepipelines.append(ap['id'])

                metadata={    
                    "title":item['attributes']['title']  ,                            
                    "field_host":item['attributes']['field_host'] ,   
                    "field_username":item['attributes']['field_username'],
                    "field_password":item['attributes']['field_password'],
                    "field_path_to_crush_agent":item['attributes']['field_path_to_crush_agent'],
                    "field_path_to_exam_data":item['attributes']['field_path_to_exam_data'],
                    "field_treat_failed_as_terminal":item['attributes']['field_treat_failed_as_terminal'],
                    "field_activated_pipelines":activepipelines ,   
                    "body":item['attributes']['body'],
                    "uuid":uuid,
                    "cms_host":self.HOST                                             
                }         

                if item['attributes']['status']==True:                            
                    projects[item['id']]=Project(metadata=metadata)   
                else:
                    print(f"Project ({item['attributes']['title']}) Ignored: Disabled/unpublished")

        return projects
