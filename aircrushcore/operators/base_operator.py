import ast
from aircrushcore.cms import *


class BaseOperator():

    def __init__(self,id,**kwargs):
        self.ID=id        
        self.Parameters=kwargs
        self.Prerequisites={}
        self.Pipeline=""
        #Set default container
        self.Container="library://dmattie/default/test-a:sha256.877c6589259369c18c0da0f56f9d344256b1ac6221203854aa2d392cc5835d92"
        self.mode="exec"
        self.command=""

        self.task_instance = TaskInstanceCollection(cms_host=self.cms_host).get_one(id)   
        
    def __str__(self):
        return f"ID:{self.ID}\nContainer:{self.Container}\nMode:{self.mode}\nCommand:{self.command}"

    def setCallingPipeline(self,pipelineID):
        self.Pipeline=pipelineID
        pass
    
    def addPrerequisite(self,Op):        
        if(isinstance(Op,BaseOperator)):            
            self.Prerequisites[Op.ID]=Op
        else:
            raise Exception("Item passed to BaseOperator.addPrerequisite was not an instance of BaseOperator") 

    def determine_parameters(self):
        
        task = TaskCollection(cms_host=self.cms_host).get_one(self.task_instance.field_task) #fetch task definition
        parms = ast.literal_eval(task.field_parameters)        
        return parms

    def get_cmshost_connection(self,kwargs):
        
        if "cms_host" in kwargs:            
            return kwargs['cms_host']
        else:
            crush_config='../crush.ini'
            aircrush=AircrushConfig(crush_config)

            crush_host=Host(
                endpoint=aircrush.config['REST']['endpoint'],
                username=aircrush.config['REST']['username'],
                password=aircrush.config['REST']['password']
                )
            return cms_host
