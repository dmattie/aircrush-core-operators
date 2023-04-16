import os
import pickle
import requests
import tempfile
from time import sleep




class host_connection:
    def __init__(self,**kwargs):
        self.endpoint=kwargs['endpoint']
        self.username=kwargs['username']
        self.password=kwargs['password']
        self.tmp=tempfile.gettempdir()

    csrf_token=""
    logout_token=""
    endpoint=""
    Session=requests.Session()

    def get_connection_token(self):
    
        picklelocation="%s/crush-session.pickle" %(self.tmp)

        if os.path.isfile(picklelocation):
            with open(picklelocation, 'rb') as f:
                self.Session = pickle.load(f)
                head={"Content-type": "application/vnd.api+json" }
                url=f"{self.endpoint}user/login_status?_format=json"            
                r = self.Session.get(url,headers=head)
                login_status=r.content.decode("utf-8")                

                if self.csrf_token=="":
                    
                    head={"Content-type": "application/json","Accept":"*/*" }
                    url=f'{self.endpoint}/session/token'
                    
                    r = self.Session.get(url)
                    self.csrf_token=r.content

                if login_status!='1':
                    head={"Content-type": "application/vnd.api+json" }
                    url=f"{self.endpoint}user/login?_format=json"            
                    payload='{"name":"%s","pass":"%s"}' %(self.username,self.password)            
                    r = self.Session.post(url, payload,headers=head)
                    self.csrf_token=r.json()['csrf_token']
                    self.logout_token=r.json()['logout_token']

                    with open(picklelocation, 'wb') as f:
                        pickle.dump(self.Session, f, pickle.HIGHEST_PROTOCOL)                
                        return(self.Session)
                
        else:
            
                
            if self.csrf_token=="":
                
                head={"Content-type": "application/json","Accept":"*/*" }
                url=f'{self.endpoint}/session/token'
                
                r = self.Session.get(url)
                self.csrf_token=r.content
                print("Received CSRF Token")
            
            if self.logout_token=="":
                
                head={"Content-type": "application/vnd.api+json" }
                url=f"{self.endpoint}user/login?_format=json"            
                payload='{"name":"%s","pass":"%s"}' %(self.username,self.password)            
                
                for iter in range(5):
                    print(f"posting payload {payload}")
                    r = self.Session.post(url, payload,headers=head)#, headers=head)#,auth=(u, p))
                    try:
                        self.csrf_token=r.json()['csrf_token']
                        self.logout_token=r.json()['logout_token']
                        print("G2G")
                        break
                        
                   # except requests.exceptions.ConnectionError:
                   #     print(f"Resource busy. Sleeping {iter} seconds")
                   #     sleep(iter)
                    except:                        
                        print(f"Connection err. Sleeping {iter} seconds")
                        print(f"ERROR:{r.json()['message']}")
                        sleep(iter)
                if self.logout_token=="":
                    #Unable to connect
                    raise Exception(f"Unable to complete connection.")
                    



            with open(picklelocation, 'wb') as f:
                pickle.dump(self.Session, f, pickle.HIGHEST_PROTOCOL)                                
                return(self.Session)

        
        return self.Session

            

