
from .host import Host
from .compute_node import ComputeNode


class ComputeNodeCollection():
    def __init__(self,cms_host:Host):
        self.HOST = cms_host

    def get_one(self,uuid:str):
        col=self.get(uuid=uuid)
        if len(col)>0:
            x = col[list(col)[0]]
            return x
        else:
            return None
    
    def get_one_by_title(self,title:str):
        col=self.get(filter=f"filter[title][value]={title}")
        if(len(col)>0):
            p = col[list(col)[0]]
            return p
        
    def get(self,**kwargs):
        Nodes={}

        if 'uuid' in kwargs:
            uuid=kwargs['uuid']        
            filter=f"filter[id][value]={uuid}"
        else:
            filter=""

        if 'filter' in kwargs:
            filter_arg=kwargs['filter']        
            
        else:
            filter_arg=""
                        

        url=f"jsonapi/node/compute_node?{filter}{filter_arg}"             

        r = self.HOST.get(url)
        if r.status_code==200:  #We can connect to CRUSH host           
              
            if len(r.json()['data'])!=0:
                not_done=True
                while not_done:

                    page_of_nodes = self._json2computenode(r.json()['data'])
                    for node in page_of_nodes:
                        Nodes[node]=page_of_nodes[node]                    

                    if 'next' in r.json()['links']:
                        r = self.HOST.get(r.json()['links']['next']['href'])
                    else:
                        not_done=False
                
            return Nodes     

    def _json2computenode(self,json):
        #for item in r.json()['data']:
        Nodes={}
        for item in json:
            if(item['type']=='node--compute_node'):

                uuid=item['id']

                metadata={    
                    "title":item['attributes']['title']  ,                            
                    "field_host":item['attributes']['field_host'] ,   
                    "field_username":item['attributes']['field_username'],
                    "field_password":item['attributes']['field_password'],
                    "field_working_directory":item['attributes']['field_working_directory'],                            
                    "body":item['attributes']['body'],
                    "uuid":uuid,
                    "cms_host":self.HOST                                             
                }                       

                Nodes[item['id']]=ComputeNode(metadata=metadata)   

        return Nodes                


