#!/bin/bash

#set -e #Exit when a command fails
#set -x #echo each command before it runs

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

if [ $# -eq 0 ];then
    printf "ERROR:At least one parameter expected\n"    
    printf "\tUSAGE:\t\trecon.sh SOURCE PIPELINE"
    printf "\n\tEXAMPLE:\trecon.sh ~/scratch/dataset/rawdata/sub-0001/ses-000A levman\n\n"
    printf "\tResults will appear in ~/scratch/dataset/derivatives/levman/sub-0001/ses-000A/\n" 
    printf "\tIf PIPELINE not specified, output made in place\n"   
    exit 1 
fi
SOURCE=$1
PIPELINE=$2

if [[ ! -d $SOURCE ]];then
    echo "ERROR: Specified source directory doesn't exist: ($SOURCE)"
    exit 1
fi


subject=$( get_subject $SOURCE )
session=$( get_session $SOURCE )    
derivatives=$( get_derivatives $SOURCE )

cd $SOURCE
if [[ ! -d "anat" ]];then
    echo "T1 Structural directory not found in $WD/anat"
    exit 1
fi

infile=$( ls anat/${subject}_${session}*.nii |head -1)

if [[ ! -f $infile ]];then
    echo "ERROR: T1 Structural volume not found (anat/${subject}_${session}*.nii)"
    exit 1
fi
####################### START OF TRANSACTON  ###################################
transactiondir=$( begin_transaction $SOURCE )
ecode=$?
if [[ ! $ecode == 0 ]];then
    echo "Unable to continue.  Error code $ecode returned in begin_transaction (helper.sh). [$transactiondir]"
    exit 1
fi
cd $transactiondir
#######################  DO THE WORK   #########################################
# Do Reconstruction here

mkdir -p Freesurfer/mri
touch Freesurfer/mri/wmparc.mgz 

#######################  VALIDATE   ############################################
# Validate the work
if [ ! -f "Freesurfer/mri/wmparc.mgz" ];then echo "ERROR: Transaction failed.  No changes committed"; exit 2; fi

if [[ ! $PIPELINE == "" ]];then
    target=$derivatives/$PIPELINE/$subject/$session
else
    target=$source
fi
######################  COMMIT THE WORK   ######################################


result=$( end_transaction $transactiondir $target)
ecode=$?
if [[ ! $result == "" ]];then 
    echo $result; 
fi
exit $ecode



