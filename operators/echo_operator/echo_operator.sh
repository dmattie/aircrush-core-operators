import datetime

class Echo(BaseOperator):
    
    def __init__(self,id,**kwargs):  

        
    def execute(self,**kwargs):
        #Invocation INFO
        now = datetime.datetime.now()
        print("\nInvoking operator: Echo===========")
        print(f"Singularity Container:{self.container}")
        print(f"Current date and time: {str(now)}")
        print("Parameters passed:")
        for k in kwargs:
            print(f"\t{k}={kwargs[k]}")
        print("End of Echo=======================\n")

        #Execute
        super().pullContainer

if __name__ == '__main__':
    e=Echo("mainecho", arg1="a1",arg2="a2")
    e.execute()