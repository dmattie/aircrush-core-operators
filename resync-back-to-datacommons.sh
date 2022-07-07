#!/bin/bash

project=$1
subject=$2
session=$3
remote=$4

echo "Re-committing $project/${subject}/${session}"

fn="sub-${subject}/sub-${subject}_ses-${session}.tar"
source="~/scratch/datacommons/projects/$project/datasets/derivatives/"
dest="~/projects/rrg-jlevman/shared/projects/$project/datasets/derivatives/"
if [[ -f $source/levman/$fn ]];then
    if [[ $remote == "" ]];then
        echo mkdir -p $dest/levman/sub-${subject}
        echo rsync $source/levman/$fn $dest/levman/$fn
    fi
else
   echo "Source not found ($source/levman/$fn)"
fi