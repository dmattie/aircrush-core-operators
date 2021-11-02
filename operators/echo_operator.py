import datetime
from aircrushcore.operators.base_operator import BaseOperator
from aircrushcore.cms import *
#from aircrushcore.controller.configuration import AircrushConfig
import json
import ast

class echo(BaseOperator):
    
    def __init__(self,id,**kwargs):  
        super(echo,self).__init__(id)        
        self.cms_host = self.get_cmshost_connection(kwargs)    
        self.task_instance = TaskInstanceCollection(cms_host=self.cms_host).get_one(id)
           

    def execute(self,**kwargs):
        #Invocation INFO
        now = datetime.datetime.now()
        self.updateStatus("running")
        try:
            print("\nInvoking operator: Echo\nStart of Echo=====================")
            
            print(f"Current date and time: {str(now)}")
            print("Instantiation Parameters passed:")
            print(f"Task instance to run: {self.task_instance.uuid}")       
            print(f"Container:{self.Container}")
            self.parms = self.determine_parameters()
            for k in self.parms:
                print(f"{k}={self.parms[k]}")
            print("End of Echo=======================\n")
            self.updateStatus("completed")
        except Exception as e:            
            self.updateStatus("failed",e)

    def updateStatus(self,status:str,detail:str=""):
        # Possible statuses
        ###########################
        # notstarted|Not Started
        # running|Running
        # failed|Failed
        # completed|Completed
        


        self.task_instance.field_status=status
        if (len(detail)>0):
            self.task_instance.body = f"{detail}\n=======================================\n{self.task_instance.body}"
        uuid=self.task_instance.upsert()