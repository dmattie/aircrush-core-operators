#!/bin/bash

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

if [ $# -eq 0 ];then
    printf "ERROR:At least one parameter expected\n"    
    printf "\tUSAGE:\t\tcrush.sh SOURCE"
    printf "\n\tEXAMPLE:\tcrush.sh ~/scratch/dataset/derivatives/levman/sub-0001/ses-000A\n\n"
    printf "\tResults will appear in place\n"       
    exit
fi
SOURCE=$1


if [[ ! -d $SOURCE ]];then
    echo "ERROR: Specified source directory does not exist: (${SOURCE})"
    exit
fi
cd $SOURCE
mkdir -p crush 
touch crush/tracts.txt