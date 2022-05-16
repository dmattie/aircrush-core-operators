from aircrushcore.cms import Project,ProjectCollection,Host
from util.setup import ini_settings

aircrush=ini_settings()

crush_host=Host(
    endpoint=aircrush.config['REST']['endpoint'],
    username=aircrush.config['REST']['username'],
    password=aircrush.config['REST']['password']
)

p_col=ProjectCollection(cms_host=crush_host)
#p=p_col.get_one(uuid='15f20818-7f27-4b94-81de-df1630e82982')
p=p_col.get_one(uuid='15f20818-7f27-4b94-81de-df1630e82982')

overrides=p.get_overrides(task_uuid="ccc1a993-b3ae-4638-af5c-b4d651ba42bb")

print(overrides)