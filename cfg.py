import configparser
 
config = configparser.ConfigParser()
config.read("/Users/dmattie/.crush.ini")
print(config.keys)
if not config.has_section("COMMONS"):
    print("Commons serction missing")
 
if config.has_option("COMMONS",'data_transfer_node'):
    print("exist")
else:
    print("doesn't exist")
