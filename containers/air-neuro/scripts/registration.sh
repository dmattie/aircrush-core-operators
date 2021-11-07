#!/bin/bash
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

if [ ! $# -eq 2 ];then
    printf "ERROR:Two parameters expected\n"    
    printf "\tUSAGE:\t\tregistration.sh SOURCE PIPELINE"
    printf "\n\tEXAMPLE:\tregistration.sh ~/scratch/dataset/rawdata/sub-0001/ses-000A levman\n\n"
    printf "\tResults will appear in ~/scratch/dataset/derivatives/levman/sub-0001/ses-000A/\n\n"    
    exit
fi
SOURCE=$1
PIPELINE=$2


if [[ ! -d $SOURCE ]];then
    echo "ERROR: Specified source directory does not exist: (${SOURCE})"
    exit
fi

subject=$( get_subject $SOURCE )
session=$( get_session $SOURCE )    
derivatives=$( get_derivatives $SOURCE )


#Lets look for T1
if [[ ! -d $SOURCE/anat ]];then
    printf "ERROR: anat directory not found in source\n"
    exit
fi
t1=$( ls $SOURCE/anat/*.nii |head -1)
if [[  ! -f $t1 ]];then
    printf "ERROR: No structural T1 image found\n"
    exit
fi

#Lets see if dwi exists
if [[ ! -d $SOURCE/dwi ]];then
    printf "ERROR: No diffusion directory (dwi) found\n"
    exit
fi
dwi=$( ls $SOURCE/dwi/*.nii |head -1 )
if [[ ! -f $dwi ]];then    
    printf "ERROR: Diffusion volume not found in dwi directory\n"
    exit
fi
mkdir -p $derivatives/$PIPELINE/$subject/$session
cp $t1 $derivatives/$PIPELINE/$subject/$session
cp $dwi $derivatives/$PIPELINE/$subject/$session
touch $derivatives/$PIPELINE/$subject/$session/reg2brain.data.nii.gz