#!/bin/bash
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"


#set -e #Exit when a command fails
#set -x #echo each command before it runs

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Registration of DWI image to T1 image"
   echo
   echo "Usage: bids [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--datasetdir  DIR                      Path to dataset directory (just above ../[source|rawdata|derivatives]/..)"
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"  
   echo "--pipeline PIPELINE                    Specify the derivative pipeline ID where the registered image and transformation matrix will be stored"
   echo "--timepoint TIMEPOINT                  [Optional] Specify which sequence/timepoint to use for" 
   echo "                                       image if multiple captured during the same exam. Default is 1."
   echo "--overwrite                            Overwrite any existing derivative files with a conflicting name"
   echo
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options


TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:,pipeline:,overwrite,timepoint:, \
             -n 'registration' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

DATASETDIR=""
SUBJECT=""
SESSION=""
PIPELINE=""
TIMEPOINT=""
OVERWRITE=0

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datasetdir ) DATASETDIR="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
    --pipeline ) PIPELINE="$2";shift 2;;
    --timepoint ) TIMEPOINT="$2";shift 2;;
    --overwrite ) OVERWRITE=1;shift;;     
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z ${SLURM_NTASKS+x} ];then
    PARALLELISM=1
else
    PARALLELISM=$SLURM_NTASKS
fi
echo "Using up to $PARALLELISM cores"

if [[ $DATASETDIR == "" ]];then
    >&2 echo "ERROR: --datasetdir not specified"
    exit 1
fi
if [[ ! -d $DATASETDIR ]];then
    >&2 echo "ERROR: dataset directory specified not found ($DATASETDIR)"
    exit 1
fi
if [[ $SUBJECT == "" ]];then
    >&2 echo "ERROR: subject not specified"
    exit 1
fi
SESSIONpath="/ses-${SESSION}/"
if [[ $SESSION == "" ]];then
    >&2 echo "WARNING: No session specified, assuming there isn't one"  
    SESSIONpath=""  
fi
if [[ $PIPELINE == "" ]];then
    >&2 echo "ERROR: pipeline ID not specified"
    exit 1
fi

if [[ $TIMEPOINT != "" ]];then    
    TIMEPOINT="_${TIMEPOINT}"
    if [[ $SESSION == "" ]];then
        SOURCE_dwi=${DATASETDIR}/rawdata/sub-${SUBJECT}/dwi/sub-${SUBJECT}${TIMEPOINT}_dwi.nii.gz
    else
        SOURCE_dwi=${DATASETDIR}/rawdata/sub-${SUBJECT}/ses-${SESSION}/dwi/sub-${SUBJECT}_ses-${SESSION}${TIMEPOINT}_dwi.nii.gz
    fi
else


shopt -s globstar  

for eachnii in ${DATASETDIR}/rawdata/sub-${SUBJECT}${SESSIONpath}/dwi/*.nii*;do
    infile=$eachnii
    break;
done

SOURCE_dwi=$eachnii

fi
if [[ $SESSION == "" ]];then
    REFERENCE=${DATASETDIR}/derivatives/freesurfer/sub-${SUBJECT}/mri/brainmask.nii
    REFERENCEmgz=${DATASETDIR}/derivatives/freesurfer/sub-${SUBJECT}/mri/brainmask.mgz
    TARGET=${DATASETDIR}/derivatives/$PIPELINE/sub-${SUBJECT}
else
    REFERENCE=${DATASETDIR}/derivatives/freesurfer/sub-${SUBJECT}/ses-${SESSION}/mri/brainmask.nii
    REFERENCEmgz=${DATASETDIR}/derivatives/freesurfer/sub-${SUBJECT}/ses-${SESSION}/mri/brainmask.mgz
    TARGET=${DATASETDIR}/derivatives/$PIPELINE/sub-${SUBJECT}/ses-${SESSION}
fi

if [[ ! -f $SOURCE_dwi ]];then
    >&2 echo "ERROR: Specified source file doesn't exist: ($SOURCE_dwi)"
    exit 1
fi

mkdir -p $TARGET
if [[ ! -d $TARGET ]];then
    >&2 echo "ERROR: Destination derivatives directory doesn't exist or cannot be created ($TARGET)"
    exit 1
fi


if [[ $OVERWRITE -eq 1 ]];then
    rm --force $TARGET/registration/*
    rm --force $TARGET/reg2brain.data.nii.gz  
    rmdir --force $TARGET/registration 
fi

if [[ -f $TARGET/reg2brain.data.nii.gz ]];then
    echo "Existing registration detected ($TARGET/reg2brain.data.nii.gz) and --overwrite not specified.  Cleaning up any residual files and Skipping registration."
    rm --force $TARGET/registration/*
    rmdir --force $TARGET/registration 
    exit 0
fi

# Clean up any pre-existing residue

rm --force $TARGET/vol*.nii.gz
rm --force $TARGET/reg2ref.vol*.nii.gz
rm --force $TARGET/vol*.RegTransform4D

#Convert reference to nii from mgz if not done already
if [[ ! -f ${REFERENCE} && -f ${REFERENCEmgz} ]];then
    #MGZ2Nifti not called yet.  lets convert inline
    mri_convert -rt nearest -nc -ns 1 $REFERENCEmgz $REFERENCE
fi
if [[ ! -f $REFERENCE ]];then
    >&2 echo "ERROR: Reference ($REFERENCE) not found.  If an .mgz file was found I would have attempted conversion first."
    exit 1
fi


FILES=vol*.n*

echo "Registering DWI [$SOURCE_dwi] to Reference [$REFERENCE]"
cp $SOURCE_dwi $TARGET
cd $TARGET
echo fslsplit $SOURCE_dwi 
fslsplit $SOURCE_dwi #sub-${SUBJECT}_ses-${SESSION}_dwi${TIMEPOINT}.nii.gz
res=$?
if [[ $res != 0 ]];then
    >&2 echo "ERROR: Failed to split $SOURCE_dwi.  Unable to continue"
    exit 1
fi

# for f in $FILES
# do
#    fbase=$(echo $f|cut -f 1 -d '.')
#    echo "flirt -in $f -ref $REFERENCE -omat $fbase.RegTransform4D -out reg2ref.$fbase.nii.gz"
#    flirt -in $f -ref $REFERENCE -omat $fbase.RegTransform4D -out reg2ref.$fbase.nii.gz
# done

#run flirt in parallel up to $PARALLELSIM times for all vols created by fslsplit
#ls vol*.n* | xargs -n1 -P$PARALLELISM -I%  fbase=$(echo $f|cut -f 1 -d '.');flirt -in % -ref $REFERENCE -omat $fbase.RegTransform4D -out reg2ref.$fbase.nii.gz

function flirt_ref() {
  fbase=$(echo $1|cut -f 1 -d '.')
  REFERENCE=$2
  echo $1
  flirt -in $1 -ref $REFERENCE -omat $fbase.RegTransform4D -out reg2ref.$fbase.nii.gz
}
#echo {1..10} | xargs -n 1 | xargs -I@ -P4 bash -c "$(declare -f flirt_ref) ; flirt_ref @ ; echo @ "

ls vol*.n* | xargs -n1 -I@ -P$PARALLELISM bash -c "$(declare -f flirt_ref) ; flirt_ref @ $REFERENCE;"


fslmerge -a reg2brain.data.nii.gz reg2ref.*

if [[ ! -f "reg2brain.data.nii.gz" ]];then
    >&2 echo "ERROR: failed to complete image registration.  Expected to see a file reg2brain.data.nii.gz produced, but didn't"
    exit 1
else
    #Discard residue
    rm vol*
    rm reg2ref*
    # mkdir -p registration
    # rm --force registration/all-volumes.tar
    # rm --force registration/reg2ref.tar
    # cd registration
    # mv vol* registration;tar -cf registration/all-volumes.tar vol* --remove-files
    # mv reg2ref* registration;tar -cf registration/reg2ref.tar reg2ref* --remove-files

fi
exit 0
