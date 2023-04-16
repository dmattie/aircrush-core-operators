from aircrushcore.cms import Session,ComputeNode,ComputeNodeCollection,SessionCollection,Host
from util.setup import ini_settings

aircrush=ini_settings()

crush_host=Host(
    endpoint=aircrush.config['REST']['endpoint'],
    username=aircrush.config['REST']['username'],
    password=aircrush.config['REST']['password']
)

cn_col=ComputeNodeCollection(cms_host=crush_host)
#p=p_col.get_one(uuid='15f20818-7f27-4b94-81de-df1630e82982')
cn=cn_col.get_one(uuid='dde80ba2-a405-4860-9bae-75126467965e')
sessions=cn.allocated_sessions()

for ses in sessions:
    print(sessions[ses].title)
