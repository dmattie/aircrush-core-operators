import configparser
import os,sys
import glob
from aircrushcore.controller.configuration import AircrushConfig
from aircrushcore.cms import Host,ProjectCollection,SubjectCollection,Subject,SessionCollection,Session

#from aircrushcore.cms.project_collection import ProjectCollection


class DataCommons():  

    def __init__(self,aircrush:AircrushConfig):

        # config = configparser.ConfigParser()
        # config.read(configfile)
        
        self.commons_path = aircrush.config['COMMONS']['commons_path']
        self.staging_path = aircrush.config['COMMONS']['staging_path']
        self.cms_host=Host(
            endpoint=aircrush.config['REST']['endpoint'],
            username=aircrush.config['REST']['username'],
            password=aircrush.config['REST']['password']
            )
        

    def initialize(self):
        print(f"Data Commons: {self.commons_path}")
        projects=f"{self.commons_path}/projects"
        
        if not os.path.exists(f"{self.commons_path}/projects"):            
            os.makedirs(projects)  

        
        return True


    def Projects(self,active_only = False):
       

        projects=os.listdir(f"{self.commons_path}/projects/")      
        if not active_only:
            return projects
        else:
            active_projects=[]
            cms_project_collection = ProjectCollection(cms_host=self.cms_host)
            cms_projects=cms_project_collection.get()
            #print(cms_project_collection)
            for dcProject in projects:
                for cmsProject in cms_projects:
                    if cmsProject==dcProject:
                        #print('active')
                        active_projects.append(dcProject)
            return active_projects

    def Subjects(self,project: str):
        subdir=f"{self.commons_path}/projects/{project}/datasets/rawdata/sub-*"
        print(f"\t\tLooking for subjects in {subdir}")
        subjects=glob.glob(subdir)
        
        for index,value in enumerate(subjects):
            subjects[index]=os.path.basename(value.replace('sub-',''))      
        print(f"\t\tfound {len(subjects)} subjects on disk")
        return subjects

    def Sessions(self,project:str,subject:str):
        sespath = f"{self.commons_path}/projects/{project}/datasets/rawdata/sub-{subject}/ses-*"
        sessions=glob.glob(sespath)
        for index,value in enumerate(sessions):
            sessions[index]=os.path.basename(value.replace('ses-','') )
        if len(sessions)==0:
            print(f"\t\t\tfound {len(sessions)} sessions in {sespath}")
        return sessions

    # def SyncWithCMS2(self):

        
    #     print("Synchronizing CMS with data commons...")
    #     cms_project_collection = ProjectCollection(cms_host=self.cms_host)
    #     cms_projects=cms_project_collection.get()
    #     dcProjects = self.Projects(active_only=True)
    #     for project in cms_projects:

    #         dc_project_exists=False
    #         for dcp in dcProjects:
    #             if (dcp==project):
    #                 dc_project_exists=True
    #                 break
    #         print(dc_project_exists)
    #         if not dc_project_exists:
    #             print(f"Project {project} found on CMS, but not in data commons.  Skipping")
    #             continue

            
    #         print(f"Processing project {cms_projects[project].title} found in CMS")

    #         ############  Look for missing subjects and sessions ##############################
    #         subjects = self.Subjects(cms_projects[project].title)
    #         cms_subject_collection = SubjectCollection(cms_host=self.cms_host,project=project)
    #         #Create any missing subjects in CMS if needed
    #         cms_subjects = cms_subject_collection.get()
    #         refresh_needed=False
    #         counter=0
    #         for subject in subjects:                
    #             if self._subject_exists(cms_subjects,subject) == None:
    #                 counter=counter+1
    #                 metadata={
    #                     "title":subject,
    #                     "field_project":project,                    
    #                     "cms_host":self.cms_host        
    #                 }    
    #                 s = Subject(metadata=metadata)                    
    #                 suid = s.upsert()
    #                 refresh_needed=True
    #         if refresh_needed:
    #             cms_subjects = cms_subject_collection.get()
    #         #With updated list of CMS subjects, look for sessions
    #         print(f"\t{counter} subjects found in data commons")
    #         cms_session_collection = SessionCollection(cms_host=self.cms_host,project=project)            
    #         cms_sessions = cms_session_collection.get()
    #         #For each dc subject

    #         for subject in subjects:
    #             #for each dc session for this subject                
    #             dc_sessions = self.Sessions(cms_projects[project].title,subject)
    #             for session in dc_sessions:
    #                 subject_uuid=self._subject_exists(cms_subjects,subject)
    #                 #If this session isn't in CMS, create it
    #                 if self._session_exists(cms_sessions,subject_uuid,session)==None:
    #                     metadata={
    #                         "title":session,
    #                         "field_participant":subject_uuid,
    #                         "field_status":"notstarted",
    #                         "cms_host":self.cms_host
    #                     }
    #                     s = Session(metadata=metadata)
    #                     s.published=False
    #                     suid = s.upsert()

    #         #################### Look for subjects and sessions on DC to add ###################
    #         # refresh_needed=False
    #         # dcSubjectsCollection = self.Subjects(cms_projects[project].field_path_to_exam_data)
    #         # for dcSubject in dcSubjectsCollection:
                
    #         #     if self._subject_exists(cms_subjects,dcSubject) == None:
    #         #         counter=counter+1
    #         #         metadata={
    #         #             "title":subject,
    #         #             "field_project":project,                    
    #         #             "cms_host":self.cms_host        
    #         #         }    
    #         #         s = Subject(metadata=metadata)                    
    #         #         suid = s.upsert()
    #         #         refresh_needed=True

    #         # if refresh_needed:
    #         #     cms_subjects = cms_subject_collection.get()

    #         # for dcSubject in dcSubjectsCollection:
    #         #     #for each dc session for this subject                
    #         #     dc_sessions = self.Sessions(cms_projects[project].title,dcSubject)
    #         #     for session in dc_sessions:
    #         #         subject_uuid=self._subject_exists(cms_subjects,dcSubject)
    #         #         #If this session isn't in CMS, create it
    #         #         if self._session_exists(cms_sessions,subject_uuid,session)==None:
    #         #             metadata={
    #         #                 "title":session,
    #         #                 "field_participant":subject_uuid,
    #         #                 "field_status":"notstarted",
    #         #                 "cms_host":self.cms_host
    #         #             }
    #         #             s = Session(metadata=metadata)
    #         #             suid = s.upsert()

    #     return True
            
                                    
    # def _subject_exists(self,collection,title):
    #     for s in collection:
    #         if collection[s].title==title:
    #             print(title)
    #             return collection[s].uuid
    #     return None
    # def _session_exists(self,collection,subject_uuid,title):
    #     for s in collection:
    #         print(f"comparing session {collection[s].title} with {title} and participant {collection[s].field_participant} with subject_uuid {subject_uuid}")
    #         if collection[s].field_participant==subject_uuid and collection[s].title==title:
    #             print("found")
    #             return collection[s].uuid
    #     return None
  
            
    def SyncWithCMS(self,**kwargs):
        republish=False
        if 'republish' in kwargs:
            republish=kwargs['republish']

        print("Synchronizing CMS with data commons...")
        cms_project_collection = ProjectCollection(cms_host=self.cms_host)
        cms_projects=cms_project_collection.get()
        dcProjects = self.Projects()
        for cms_project_uid in cms_projects:

            dc_project_exists=False
            for dcp in dcProjects:
                if (dcp==cms_projects[cms_project_uid].field_path_to_exam_data):
                    dc_project_exists=True
                    break
            
            if not dc_project_exists:
                print(f"\tCMS project {cms_projects[cms_project_uid].field_path_to_exam_data} missing in data commons.  Expected {self.commons_path}/projects/{cms_projects[cms_project_uid].field_path_to_exam_data}.  Skipping")
                continue
            else:
                print(f"\tFound {dcp}")

            dc_subject_collection = self.Subjects(cms_projects[cms_project_uid].field_path_to_exam_data)
            cms_subject_collection = SubjectCollection(cms_host=self.cms_host,project=cms_project_uid)            
            cms_subjects = cms_subject_collection.get()
            #Upsert all DC subjects
            refresh_needed=False
            
            for dc_subject in dc_subject_collection:
                #Exist on cms?
                cms_has_subject=False
                for cms_subject in cms_subjects:
                    if cms_subjects[cms_subject].title==dc_subject:
                        cms_has_subject=True
                        if cms_subjects[cms_subject].published==False:
                            if republish:
                                cms_subjects[cms_subject].published=True                            
                                cms_subjects[cms_subject].upsert()
                            else:
                                print(f"[INFO] {cms_subjects[cms_subject].title} is unpublished but exists on data commons.  Consider re-publishing.")
                        break
                if not cms_has_subject:
                    #Upsert it
                    metadata={
                        "title":dc_subject,
                        "field_project":cms_project_uid,                    
                        "cms_host":self.cms_host        
                    }    
                    s = Subject(metadata=metadata)                    
                    suid = s.upsert()
                    refresh_needed=True
            #Disable any CMS subjects not in DC
            for cms_subject in cms_subjects:
                dc_has_subject=False
                for dc_subject in dc_subject_collection:
                    if dc_subject==cms_subjects[cms_subject].title:
                        dc_has_subject=True
                        break
                if not dc_has_subject:
                    cms_subjects[cms_subject].published=False                    
                    cms_subjects[cms_subject].upsert()
                    print(f"{cms_subjects[cms_subject].title} has been unpublished because it is no longer on data commons")
                    refresh_needed=True

            #For all active CMS subjects, get sessions
            if refresh_needed:
                cms_subjects = cms_subject_collection.get()
            
            print("Looking for sessions")
            for cms_subject in cms_subjects:
                dc_sessions = self.Sessions(cms_projects[cms_project_uid].field_path_to_exam_data, cms_subjects[cms_subject].title)
                
                cms_session_collection = SessionCollection(cms_host=self.cms_host,project=cms_project_uid,subject=cms_subject)            
                cms_sessions = cms_session_collection.get()
                
                refresh_needed=False
                for dc_session in dc_sessions:
                    cms_has_session=False
                    for cms_session in cms_sessions:
                        if cms_sessions[cms_session].title==dc_session:
                            if cms_sessions[cms_session].published==False:
                                if republish:
                                    cms_sessions[cms_session].published=True
                                    cms_sessions[cms_session].upsert()
                                else:
                                    print(f"Session {cms_sessions[cms_session].title} is unpublished but exists on data commons. Consider re-publishing")
                            cms_has_session=True
                            break
                    if not cms_has_session:
                        metadata={
                            "title":dc_session,
                            "field_participant":cms_subject,
                            "field_status":"notstarted",
                            "cms_host":self.cms_host
                        }
                        s = Session(metadata=metadata)
                        s.published=True
                        suid = s.upsert()
                        refresh_needed=True
                if refresh_needed:
                    cms_sessions = cms_session_collection.get()

                for cms_session in cms_sessions:
                    dc_has_session=False
                    for dc_session in dc_sessions:
                        if cms_sessions[cms_session].title==dc_session:
                            dc_has_session=True
                            if cms_sessions[cms_session].published==False:
                                if republish:
                                    cms_sessions[cms_session].published=True
                                    cms_sessions[cms_session].upsert()
                                else:
                                    print(f"Session {cms_sessions[cms_session].title} is unpublished but exists on data commons. Consider re-publishing")
                            
                            break
                    if not dc_has_session:
                        cms_sessions[cms_session].published=False
                        cms_sessions[cms_session].upsert()
        print("Sync completed.")
        return True


# if __name__ == '__main__':
#     homedir=os.path.expanduser('~')
#     crush_config=f"{homedir}/.crush.ini"
#     aircrush=AircrushConfig(crush_config)
#     dc=DataCommons(aircrush)
#     dc.SyncWithCMS()
    


