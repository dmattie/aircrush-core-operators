#!/bin/bash
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

#set -e #Exit when a command fails
set -x #echo each command before it runs

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Reshape source into BIDS compliant rawdata (best effort)"
   echo
   echo "Usage: bids [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--datasetdir  DIR                      Path to dataset directory (just above ../[source|rawdata|derivatives]/..)"
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"   
   echo
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options

TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:, \
             -n 'bids' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

DATASETDIR=""
SUBJECT=""
SESSION="1"

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datasetdir ) DATASETDIR="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
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
############################################
# CASE A
############################################
case_a_test(){  ## ABIDE
  echo "[$SESSION]"  
  if [[ -d $DATASETDIR/source/$SUBJECT 
  && -d $DATASETDIR/source/$SUBJECT/session_$SESSION
  && -d $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1 
  && -f $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1/anat.nii.gz ]];then
    return 0
  fi
  return 1
}
case_a_autobids(){  ##ABIDE LOOK-A-LIKE
  echo "It looks like source matches pattern 'A'. Applying autobids rules for pattern 'A'"
  mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat  
  cp $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1/anat.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat/sub-${SUBJECT}_ses-${SESSION}_anat.nii.gz
  if [[ -d $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1 ]];then
    mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1/dti.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/sub-${SUBJECT}_ses-${SESSION}_dwi.nii.gz
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvals $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvals
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvecs_image $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvecs
  fi
  return 0
}
############################################
TARGET=$DATASETDIR/rawdata/

if case_a_test;then
  if case_a_autobids;then
    exit 0
  else
    echo "[ERROR] Pattern A attempted but failed"
  fi
fi

echo "[WARNING] Source does not match any convertible pattern.  Manual conversion to BIDS needed. Unable to continue"
exit 1
