#!/bin/bash
project=$1
subject=$2
session=$3


if [[ -d ~/scratch/datacommons/projects/$project/datasets/rawdata/sub-${subject}/ses-${session} || -f ~/scratch/datacommons/projects/$project/datasets/rawdata/sub-${subject}/sub-${subject}_ses-${session}.tar.deleteme ]];then
    echo "Found ~/scratch/datacommons/projects/$project/datasets/rawdata/sub-${subject}/ses-${session}"
fi 
if [[  -d ~/scratch/datacommons/projects/$project/datasets/derivatives/levman/sub-${subject}/ses-${session} || -f ~/scratch/datacommons/projects/$project/datasets/derivatives/levman/sub-${subject}/sub-${subject}_ses-${session}.tar.deleteme ]];then
    echo "Found ~/scratch/datacommons/projects/$project/datasets/derivatives/levman/sub-${subject}/ses-${session}"
fi
if [[ -d ~/scratch/datacommons/projects/$project/datasets/derivatives/freesurfer/sub-${subject}/ses-${session} || -f ~/scratch/datacommons/projects/$project/datasets/derivatives/freesurfer/sub-${subject}/sub-${subject}_ses-${session}.tar.deleteme ]];then
    echo "Found ~/scratch/datacommons/projects/$project/datasets/derivatives/freesurfer/sub-${subject}/ses-${session}"
fi

