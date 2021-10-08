#!/bin/bash
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

if [ ! $# -eq 2 ];then
    printf "ERROR:Two parameters expected\n"    
    printf "\tUSAGE:\t\tparcellate.sh SOURCE PIPELINE"
    printf "\n\tEXAMPLE:\tparcellate.sh ~/scratch/dataset/rawdata/sub-0001/ses-000A levman\n\n"
    printf "\tResults will appear in ~/scratch/dataset/derivatives/levman/sub-0001/ses-000A/\n\n"    
    exit
fi
SOURCE=$1
PIPELINE=$2

if [[ ! -d $SOURCE ]];then
    echo "ERROR: Specified source directory doesn't exist: ($SOURCE)"
    exit
fi

cd $SOURCE

subject=$( get_subject $SOURCE )
session=$( get_session $SOURCE )    
derivatives=$( get_derivatives $SOURCE )
echo "$PIPELINE"
if [[ ! $PIPELINE == "" ]];then
    mkdir -p $derivatives/$PIPELINE/$subject/$session
    cd $derivatives/$PIPELINE/$subject/$session
else
    echo "Rendering in place"
fi

mkdir -p parcellations
touch parcellations/wmparc0002.nii
touch parcellations/wmparc0004.nii
touch parcellations/wmparc0005.nii