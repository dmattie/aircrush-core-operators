#!/bin/bash
# source=$1
# target=$2
# rsync -r $1/ $2

#set -e #Exit when a command fails
set -x #echo each command before it runs

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Tar and copy data from the working directory to the data commons."
   echo
   echo "Usage: push_dataset [-h] [OPTIONS...]"
   echo "options:"
   echo "--h                                    Print this Help."
   echo "--datacommons DIR                      Path to data commons root"
   echo "--workingdir  DIR                      Path to working directory"
   echo "--project PROJECT                      Project ID.  If subject and/or session not"
   echo "                                       specified, clone the entire project"
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"
   echo "--pipeline PIPELINE                    If stage is set to derivatives, pipeline ID must be specified"
   echo
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options

TEMP=`getopt -o h: --long help,project:,datacommons:,workingdir:,subject:,session:,pipeline: \
             -n 'push_dataset' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

PROJECT=""
DATACOMMONS=""
WORKINGDIR=""
SUBJECT=""
SESSION=""
SESSIONPATH=""
PIPELINE=""

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datacommons ) DATACOMMONS="$2"; shift 2 ;;
    --workingdir ) WORKINGDIR="$2";shift 2;; 
    --stage ) STAGE="$2";shift 2;;     
    --project ) PROJECT="$2";shift 2;;
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
    --pipeline ) PIPELINE="$2";shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done


if [[ $PROJECT == "" ]];then
    >&2 echo "ERROR: --project not specified"
    exit 1
fi

if [[ $WORKINGDIR == "" ]];then
    >&2 echo "ERROR: --workingdir not specified"
    exit 1
fi
if [[ ! -d $WORKINGDIR ]];then
    >&2 echo "ERROR: Working directory specified not found"
    exit 1
fi
if [[ $DATACOMMONS == "" ]];then
    >&2 echo "ERROR: --datacommons not specified"
    exit 1
fi
if [[ ! $SESSION == "" ]];then
   SESSIONPATH="ses-$SESSION"
   FILENAME="sub-$SUBJECT_$SESSIONPATH.tar"
else
   FILENAME="sub-$SUBJECT.tar"
fi

SOURCE=$WORKINGDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONPATH
TARGET=$DATACOMMONS/$PROJECT/datasets/derivatives/$PIPELINE

if [[ -d $WORKINGDIR/derivatives/$PIPELINE ]];then
    cd $WORKINGDIR/derivatives/$PIPELINE
else
    >&2 echo "ERROR: Derivatives directory not found ($WORKINGDIR/derivatives/$PIPELINE)"
    exit 1
fi

if [[ -f $WORKINGDIR/derivatives/$PIPELINE/$FILENAME && ! -d $SOURCE ]];then
    echo "Tar exists and dir doesn't.  sending tar  ($WORKINGDIR/derivatives/$PIPELINE/$FILENAME)"
elif [[ -f $WORKINGDIR/derivatives/$PIPELINE/$FILENAME && -d $SOURCE ]];then
    echo "Tar exists and so does dir.  removing dir and sending tar  ($WORKINGDIR/derivatives/$PIPELINE/$FILENAME)"
elif [[ ! -f $WORKINGDIR/derivatives/$PIPELINE/$FILENAME && -d $SOURCE ]];then
    echo "Creating tar ($WORKINGDIR/derivatives/$PIPELINE/$FILENAME) and removing dir ($SOURCE)"
    tar -cf $WORKINGDIR/derivatives/$PIPELINE/$FILENAME $SOURCE

elif [[  ! -f $WORKINGDIR/derivatives/$PIPELINE/$FILENAME && ! -d $SOURCE ]];then
    echo "No tar found ($WORKINGDIR/derivatives/$PIPELINE/$FILENAME) and dir doesn't exist ($SOURCE)"
fi

if [[ $? -eq 0 ]];then
 rsync $WORKINGDIR/derivatives/$PIPELINE/$FILENAME $TARGET
fi
#rsync -r $SOURCE $TARGET
#echo "rsync ran for $SECONDS seconds"


