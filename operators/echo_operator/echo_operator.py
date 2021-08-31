import datetime

class Echo():
    
    def __init__(self,id,**kwargs):  
        pass
        
    def execute(self,**kwargs):
        #Invocation INFO
        now = datetime.datetime.now()
        print("\nInvoking operator: Echo\nStart of Echo=====================")
        
        print(f"Current date and time: {str(now)}")
        print("Parameters passed:")
        for k in kwargs:
            print(f"\t{k}={kwargs[k]}")
        print("End of Echo=======================\n")

        

if __name__ == '__main__':
    e=Echo("mainecho")
    e.execute(arg1="a1",arg2="a2")