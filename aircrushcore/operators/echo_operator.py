from .base_operator import BaseOperator
import datetime
import json

class Echo(BaseOperator):
    
    def __init__(self,id,**kwargs):  
        super().__init__()
        self.container="library://dmattie/default/test-a:sha256.877c6589259369c18c0da0f56f9d344256b1ac6221203854aa2d392cc5835d92"              
        self.mode="exec"        
        self.parameters=None
        self.command=f"""echo 'ECHO Operator';
echo \"Invoking operator: Echo===========\";
echo \"Singularity Container:{self.container}\";
echo \"Current date and time: {str(now)}\";
echo \"Parameters passed:\";
"""
        
        if "parameters" in kwargs:
            self.parameters=json.loads(kwargs['parameters'])
            
            for k in self.parameters:
                self.command = self.command + f"echo \"\t{k}={self.parameters[k]}\";"

        

        
    # def execute(self,**kwargs):
    #     #Invocation INFO
    #     now = datetime.datetime.now()

    #     for k in kwargs:
    #         print(f"\t{k}={kwargs[k]}")
    #     print("End of Echo=======================\n")

    #     #Execute
    #     super().pullContainer
