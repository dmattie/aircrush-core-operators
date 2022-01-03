#!/bin/bash

source "${SCRIPTPATH}/lib/crush/crush_import.sh"
#set -e #Exit when a command fails
#set -x #echo each command before it runs

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Cartesian product of Region to Region tract measurement extraction"
   echo
   echo "Usage: recon [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--datasetdir  DIR                      Path to dataset directory (just above ../[source|rawdata|derivatives]/..)"
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"  
   echo "--pipeline PIPELINE_ID                 Specify derivatives directory to store output.  If unspecified, store in crush." 
   echo "--gradientmatrix GRADIENTMATRIX        If a gradient matrix file has been provided, specifiy its location here"
   echo
}


############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options

TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:,pipeline:,gradientmatrix:,bmax:,b0:,\
             -n 'crush' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

DATASETDIR=""
SUBJECT=""
SESSION=""
PIPELINE=""
GRADIENTMATRIX=""
BMAX=""
BNOT=""

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datasetdir ) DATASETDIR="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
    --pipeline ) PIPELINE="$2";shift 2;;
    --gradientmatrix ) GRADIENTMATRIX="$2";shift 2;;
    --bmax ) BMAX="$2";shift 2;;
    --BNOT ) BNOT="$2";shift 2;;    
    -- ) shift; break ;;
    * ) break ;;
  esac
done


if [[ $DATASETDIR == "" ]];then
    >&2 echo "ERROR: --datasetdir not specified"
    exit 1
fi
if [[ ! -d $DATASETDIR ]];then
    >&2 echo "ERROR: dataset directory specified not found ($datasetdir)"
    exit 1
fi
if [[ $SUBJECT == "" ]];then
    >&2 echo "ERROR: subject not specified"
    exit 1
fi

if [[ $PIPELINE == "" ]];then
    >&2 echo "ERROR: pipeline not specified"
    exit 1
fi
if [[ $SESSION == "" ]];then
    >&2 echo "ERROR: session not specified"
    exit 1
fi

if [[ $GRADIENTMATRIX != "" && ! -f $GRADIENTMATRIX ]];then
    >&2 echo "ERROR: A gradient matrix has been specified but cannot be found ($GRADIENTMATRIX)"
    exit 1
fi
  

SOURCE=$DATASETDIR/rawdata/sub-$SUBJECT/ses-$SESSION
TARGET=$DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION
  
if [[ ! -d $SOURCE ]];then
    >&2 echo "ERROR: Specified source directory doesn't exist: ($SOURCE)"
    exit 1
fi

res=$( creategradientmatrix $TARGET/gradientmatrix.txt )
if [[ "$res" -ne "0" ]];then
    >&2 echo "ERROR: Unable to establish a gradient matrix.  Unable to continue."
fi

###########################
# HARDI_MAT               #
###########################

 hardi_mat $TARGET/gradientmatrix.txt $TARGET/temp_mat.dat -ref $TARGET/reg2brain.data.nii.gz
 res=$?

 if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to perform hardi_mat.  Unable to continue."
    exit 1
 fi
            

###########################
# RECON                   #
###########################

diffusion_recon $SOURCE $TARGET
res=$?

if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to perform Cortical Reconstruction.  Unable to continue."
    exit 1
fi



#python $CRUSH_PATH/crush.py -samples $SUBJECTS_DIR -patient sub-$patientID -recrush -fixmissing #-gradienttable ~/projects/def-dmattie/crush/plugins/levman/hcp_gradient_table_from_data_dictionary_3T.csv
#pwd

#if [ -f "$SUBJECTS_DIR/sub-$patientID/ses-$sessionID/Tractography/crush/tracts.txt" ]; then