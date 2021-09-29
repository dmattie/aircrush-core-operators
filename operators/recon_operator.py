import datetime
from aircrushcore.operators.base_operator import BaseOperator
from aircrushcore.cms.models import *
from aircrushcore.controller.configuration import AircrushConfig
import json
import ast

class recon(BaseOperator):
    
    def __init__(self,id,**kwargs):  
        super(recon,self).__init__(id)        
        self.cms_host = self.get_cmshost_connection(kwargs)    
        self.task_instance = TaskInstanceCollection(cms_host=self.cms_host).get_one(id)   
        

    def execute(self,**kwargs):
        #Invocation INFO
        messages=[]
        now = datetime.datetime.now()
        self.updateStatus("running")

        try:
            messages.append("Invoking operator: recon =====================")
            
            messages.append(f"Current date and time: {str(now)}")
            messages.append("Instantiation Parameters passed:")
            messages.append(f"Task instance to run: {self.task_instance.uuid}")       
            messages.append(f"Container:{self.Container}")
            self.parms = self.determine_parameters()
            for k in self.parms:
                messages.append(f"{k}={self.parms[k]}")               
            messages.append("End of recon=======================\n")
            self.updateStatus("completed",'\n'.join(messages),"")
        except Exception as e:
            if hasattr(e, 'message'):
                new_errors=e.message
            else:
                new_errors=str(e)
            self.updateStatus("failed",'\n'.join(messages),new_errors)
        


    def updateStatus(self,status:str,detail:str="",new_errors:str=""):
        # Valid statuses
        ###########################
        # notstarted,running,failed,completed
        self.task_instance.field_status=status        
        self.task_instance.body = detail
        self.task_instance.field_errorlog = new_errors

        uuid=self.task_instance.upsert()
