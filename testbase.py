import os,sys

cwd=os.getcwd()

print(cwd)

#print(os.path.dirname(cwd))
ses=os.path.basename(cwd)
sub=os.path.basename(os.path.dirname(cwd))
root=os.path.dirname(os.path.dirname(cwd))

if ses[0:4]=="ses-":
    print(ses)
    sespart=f"_{ses}"
else:
    sespart=""

if sub[0:4]=="sub-":
    target=f"{root}/{sub}{sespart}.tar"

print(target)
