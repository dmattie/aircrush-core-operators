from aircrushcore.cms.project import Project
from aircrushcore.cms.project_collection import ProjectCollection
from aircrushcore.cms.session_collection import SessionCollection
from aircrushcore.cms.subject_collection import SubjectCollection
from aircrushcore.cms.subject import Subject
from aircrushcore.cms.session import Session
from aircrushcore.datacommons.data_commons import DataCommons
from aircrushcore.controller.configuration import AircrushConfig
from aircrushcore.cms.host import Host
import asyncio, asyncssh, sys
import traceback
import requests
import json

class Sync():
    def __init__(self,aircrush:AircrushConfig):
        self.aircrush=aircrush 

        self.crush_host=Host(
            endpoint=aircrush.config['REST']['endpoint'],
            username=aircrush.config['REST']['username'],
            password=aircrush.config['REST']['password']
            )       

    def sync_projects(self):
        dc=DataCommons(self.aircrush)

   
    async def _run_project_status_client(self,host:str,username:str,password:str,cmd:str,uuid:str,crushHOST:Host):
    

        async with asyncssh.connect(host,username=username, password=password, known_hosts=None) as conn:            
            try:
                agentresult = await conn.run(cmd, check=True)
            except:
                print(f"ERROR: status command failed {cmd}")
                return
            
            
            project=uuid

            proj_collection=ProjectCollection(cms_host=crushHOST)
            project_object=proj_collection.get_one(uuid)
            
            subject_collection=SubjectCollection(cms_host=crushHOST,project=uuid)
            

            subjects=subject_collection.get(project=project)
            
            try:
                agentExams=json.loads(agentresult.stdout)
            except:                
                traceback.print_exc()                
                raise Exception("ERROR:sync_participant::Agent didn't return JSON")
            
            newnodes=0
            updatednodes=0
            
            

            #Iterate exams in Project Directory
            for participant in agentExams['participants']: 

                isbids="unknown"           
                subject_metadata={
                        "title":agentExams['participants'][participant]['id'],
                        "isbids":isbids,
                        "field_project":project,
                        "uuid":None,
                        "cms_host":self.crush_host            
                    }                
                
                #Look for existing subject
                
                found_matching_subject_guid_in_cms=None
                for subject_guid in subjects:
                    subject=subjects[subject_guid]  
                                          
                    if participant==subject.title:  
                        print(f"match found:  update of data commons participant={participant}, CMS subject title={subject.title}.  Subject UUID:{subject.uuid}")                        
                        found_matching_subject_guid_in_cms=subject.uuid
                        break   
                
                if not found_matching_subject_guid_in_cms is None:                    
                    subject_metadata['uuid']=subject.uuid      
              
                              
                s = Subject(metadata=subject_metadata)                
                participant_uuid=s.upsert()                                                                  
                #Upsert any associated sessions--------------------------------
                session_collection=SessionCollection(cms_host=crushHOST,subject=participant_uuid)                
                sessions=session_collection.get()                                                
                for data_commons_sessions in agentExams['participants'][participant]['sessions']:                                                
                    session_metadata={
                            "title":data_commons_sessions,
                            "field_participant":participant_uuid,
                            # "field_status":"notstarted",    
                            "uuid":None,                                
                            "cms_host":self.crush_host
                        }  
                    sessionExists=False

                    #Look for existing session
                    for cms_session_guid in sessions:
                        cms_session=sessions[cms_session_guid]                        
                        if data_commons_sessions==cms_session.title: 
                            print(f"Matched data_commons_session:{data_commons_sessions}, cms_session.title:{cms_session.title}")                           
                            session_metadata['uuid']=cms_session.uuid
                            
                    if not session_metadata['uuid']:
                        print(f"No session found in CMS. Creating Session {data_commons_sessions} for subject {s.title} in {project_object.title}")
                    #else:
                       # print(f"Session found in CMS. Updating Session  for {s.title} in {project_object.title}")

                    #print(session_metadata)
                    ses = Session(metadata=session_metadata)                    
                    ses.upsert()     

                #Unpublish any sessions not found
                for session_guid in sessions:
                    if sessions[session_guid].title not in agentExams['participants'][participant]['sessions']:
                        sessions[session_guid].published=False
                        sessions[session_guid].upsert()

            #Unpublish any subjects not found
            for subject_guid in subjects:
                if subjects[subject_guid].title not in agentExams['participants']:
                    subjects[subject_guid].published=False
                    subjects[subject_guid].upsert()

    
    def sync_subject_sessions(self):
                  
        dc=DataCommons(self.aircrush)              
        proj_collection=ProjectCollection(cms_host=self.crush_host)  
        project_list=dc.Projects(active_only=True)        
        
        cms_projects = proj_collection.get()
        project=None
        for project_uuid in cms_projects:
            project = proj_collection.get_one(project_uuid)            
            cmd=f"python3.8 {project.field_path_to_crush_agent}/ps2.py {project.field_path_to_exam_data}"
            print(f"Project Sync Initiated:{project.title}")
            print("-----------------------------------------------------------")
            asyncio.get_event_loop().run_until_complete(
                    self._run_project_status_client(
                        host=project.field_host,
                        username=project.field_username,
                        password=project.field_password,                
                        cmd=cmd,
                        uuid=project.uuid,
                        crushHOST=self.crush_host
                    )
                )  
                                     
                                
