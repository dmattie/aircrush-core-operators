#!/bin/bash

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
   echo "Reconstruct structural T1 image using Freesurfer recon_all"
   echo
   echo "Usage: recon [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--datasetdir  DIR                      Path to dataset directory (just above ../[source|rawdata|derivatives]/..)"
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"  
   echo "--pipeline PIPELINE_ID                 Specify derivatives directory to store output.  If unspecified, store in rawdata." 
   echo
}


############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options


TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:,pipeline:, \
             -n 'recon' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

DATASETDIR=""
SUBJECT=""
SESSION=""
PIPELINE=""

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datasetdir ) DATASETDIR="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
    --pipeline ) PIPELINE="$2";shift 2;;
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
if [[ $SESSION == "" ]];then
    SOURCE=$DATASETDIR/rawdata/sub-$SUBJECT
    if [[ $PIPELINE =="" ]];then
        TARGET=$DATASETDIR/rawdata/freesurfer/sub-$SUBJECT
    else
        TARGET=$DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT
    fi
else
    SOURCE=$DATASETDIR/rawdata/sub-$SUBJECT/ses-$SESSION
    if [[ $PIPELINE =="" ]];then
        TARGET=$DATASETDIR/derivatives/freesurfer/sub-$SUBJECT/ses-$SESSION
    else
        TARGET=$DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION
        
    fi
    
fi

if [[ ! -d $SOURCE ]];then
    >&2 echo "ERROR: Specified source directory doesn't exist: ($SOURCE)"
    exit 1
fi

cd $SOURCE
if [[ ! -d "anat" ]];then
    >&2 echo "T1 Structural directory not found in $SOURCE/anat"
    exit 1
fi
cd $SOURCE

infile=$( ls anat/sub-${SUBJECT}*.nii |head -1)

if [[ ! -f $infile ]];then
    if [[ ! -f "${infile}.gz" ]];then
        >&2 echo "ERROR: T1 Structural volume not found (anat/sub-${SUBJECT}*.nii.gz)"
        exit 1
    else
        infile="${infile}.gz"
    fi
fi

#######################  DO THE WORK   #########################################
# Do Reconstruction here

mkdir -p $TARGET

cd $SOURCE/anat

infile=$( ls sub-${SUBJECT}_ses-${SESSION}*.nii )
infile_gz=$( ls sub-${SUBJECT}_ses-${SESSION}*.nii.gz )
if [[ ! -f $SOURCE/anat/$infile && -f $infile_gz ]];then
   infile=$infile_gz
   echo "found compressed version: $SOURCE/anat/$infile"
fi  

cd $TARGET
# singularity exec --bind $DATASETDIR:/dataset \
#     $AIRCRUSH_CONTAINERS/air-neuro.sif \
#     /usr/local/freesurfer/7.2.0/bin/recon-all \
#     -s freesurfer  \
#     -i /dataset/rawdata/sub-29152/ses-1/anat/sub-29152_ses-1_anat.nii.gz
SUBJECTS_DIR=$TARGET
echo "Performing cortical reconstruction of $SOURCE/anat/$infile"
recon-all -s freesurfer -i $SOURCE/anat/$infile -all 
if [[ $? -eq 0 && -f $TARGET/freesurfer/mri/wmparc.mgz ]];then
    mv $TARGET/freesurfer/* $TARGET
    rmdir $TARGET/freesurfer

    shopt -s globstar  
    for eachmgz in $TARGET/*.mgz;do
        if [[ -f $eachmgz ]];then        
            mgz_to_nifti $eachmgz
        fi
    done  

    # Convert mgz 2 nifti files

    echo "recon-all complete"
fi

# mkdir -p $TARGET/mri
# touch $TARGET/mri/wmparc.mgz 
# ecode=$?

#######################  VALIDATE   ############################################
# Validate the work
if [ ! -f "$TARGET/mri/wmparc.mgz" ];then 
    >&2 echo "ERROR: recon_all failed.  No changes committed"; 
    exit 1; 
fi


exit



