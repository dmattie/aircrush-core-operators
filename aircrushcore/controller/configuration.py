import configparser

class AircrushConfig():
    def __init__(self,configfile:str):

        self.config = configparser.ConfigParser()
        self.config.read(configfile)

