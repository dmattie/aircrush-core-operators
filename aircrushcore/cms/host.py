from .authentication import host_connection
import sys
import requests
import json
from requests_toolbelt.utils import dump


class Host:
    def __init__(self,username:str, password:str ,endpoint:str):            
        self.username=username
        self.password=password
        self.endpoint=endpoint    
        print(f"Connecting to CMS ({endpoint})...") 
        self.connection=host_connection(
          endpoint=self.endpoint,
          username=self.username,
          password=self.password)
        try:
          self.session=self.connection.get_connection_token()
        except Exception as e:
          raise Exception(f"Unable to make a conection. ({e})")
        #print(f"host.session={self.session}")

    def get(self,url):
      head = {"Accept":"application/vnd.api+json","Content-Type":"application/vnd.api+json","X-CSRF-Token":self.connection.csrf_token}
      if url[0:4]!="http":
        url=f"{self.connection.endpoint}{url}"        

      r = self.session.get(url, headers=head)
      return r

    def patch (self,url,payload):
      head = {"Accept":"application/vnd.api+json","Content-Type":"application/vnd.api+json","X-CSRF-Token":self.connection.csrf_token}
      url=f"{self.connection.endpoint}{url}"
      r = self.session.patch(url, json.dumps(payload), headers=head)
      # data = dump.dump_all(r)
      # print(data.decode('utf-8'))
      
      return r

    def post (self,url,payload):
      head = {"Accept":"application/vnd.api+json","Content-Type":"application/vnd.api+json","X-CSRF-Token":self.connection.csrf_token}
      url=f"{self.connection.endpoint}{url}"
      #print(self.session)
      r = self.session.post(url, json.dumps(payload), headers=head)
      #data = dump.dump_all(r)
      #print(data.decode('utf-8'))      
      return r      

    def delete (self,url):
      head = {"Accept":"application/vnd.api+json","Content-Type":"application/vnd.api+json","X-CSRF-Token":self.connection.csrf_token}
      url=f"{self.connection.endpoint}{url}"      
      r = self.session.delete(url, headers=head)

      return r
      
