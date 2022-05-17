
from aircrushcore.cms import *
import configparser
from aircrushcore.controller.configuration import AircrushConfig
import json

class Health():
    def __init__(self,aircrush:AircrushConfig):
        self.aircrush=aircrush 

        self.crush_host=Host(
            endpoint=aircrush.config['REST']['endpoint'],
            username=aircrush.config['REST']['username'],
            password=aircrush.config['REST']['password']
            )  
        self.proj_collection=ProjectCollection(cms_host=self.crush_host)             
        self.sub_collection=SubjectCollection(cms_host=self.crush_host)
        self.ses_collection=SessionCollection(cms_host=self.crush_host)
        self.task_collection=TaskCollection(cms_host=self.crush_host)
        self.task_instance_collection=TaskInstanceCollection(cms_host=self.crush_host)
        self.pipeline_collection=PipelineCollection(cms_host=self.crush_host)

    def audit(self):
        #self.check_projects()
        self.check_subjects()
        self.check_sessions()
        self.check_task_instances()

    def check_projects(self):
         
        all_projects = self.proj_collection.get()
        for project in all_projects:
            print(project)

    def check_subjects(self):
        orphan_count=0
        all_subject_uuids=self.sub_collection.get()
        for subuuid in all_subject_uuids:
            subject = self.sub_collection.get_one(subuuid)
            project = self.proj_collection.get_one(subject.field_project)
            if( not project):
                print(f"\tSubject {subuuid} has been orphaned.  Project {project} does not exist.  Unpublishing subject")
                subject.published=False
                subject.field_project=None
                subject.upsert()
                orphan_count += 1
        print(f"{orphan_count} orphaned subjects detected")
        

    def check_sessions(self):
        orphan_count=0
        all_session_uuids = self.ses_collection.get()
        for sesuuid in all_session_uuids:
            session = self.ses_collection.get_one(sesuuid)
            subject = self.sub_collection.get_one(session.field_participant)
            if(not subject):
                print(f"Session {sesuuid} has been orphaned.  Subject {subject} does not exist.  Unpublishing session")
                session.published=False
                session.field_subject=None
                session.upsert()
                orphan_count += 1
        print(f"{orphan_count} orphaned sessions detected")

    def check_tasks(self):
        pass

    def check_task_instances(self):
        orphan_count=0
        all_ti_uuids = self.task_instance_collection.get()
        for tiuuid in all_ti_uuids:
            ti = self.task_instance_collection.get_one(tiuuid)
            session = self.ses_collection.get_one(ti.field_associated_participant_ses)
            pipeline = self.pipeline_collection.get_one(ti.field_pipeline)
            task = self.task_collection.get_one(ti.field_task)
            if (not session or not pipeline or not task):
                print(f"Task Instance {tiuuid} has been orphaned.")
            if (not session):
                print(f"\tSession:{ti.field_associated_participant_ses} does not exist")
                ti.field_associated_participant_ses=None
                ti.published=False
            if (not pipeline):                
                print(f"\tPipeline:{ti.field_pipeline} does not exist")
                ti.field_pipeline=None
                ti.published=False
            if (not task):
                print(f"\ttask:{ti.field_task} does not exist")
                ti.field_task=None
                ti.published=False
            if (not session or not pipeline or not task):
                print("Upserting")
                ti.upsert()
                orphan_count += 1
        print(f"{orphan_count} orphaned task instances detected")
                            




