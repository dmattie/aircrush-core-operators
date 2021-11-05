#!/bin/bash
# source=$1
# target=$2
# rsync -r $1/ $2

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
   echo "Copy data from the data commons to local working directory."
   echo
   echo "Usage: pull_dataset [-h] [OPTIONS...]"
   echo "options:"
   echo "--h                                    Print this Help."
   echo "--datacommons DIR                      Path to data commons root"
   echo "--stage [source|rawdata|derivatives]   Stage to clone. All if omitted"
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

TEMP=`getopt -o h: --long help,project:,datacommons:,workingdir:,subject:,session:,stage:,pipeline: \
             -n 'pull_dataset' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

PROJECT=""
DATACOMMONS=""
STAGE=""
WORKINGDIR=""
SUBJECT=""
SESSION=""
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

if [[ $STAGE != "derivatives" && $PIPELINE != "" ]];then
    >&2 echo "ERROR: --pipeline specified, but --stage set to something other than derivatives or not set"
    exit 1
fi

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

if [[ $STAGE == "" && ( $SUBJECT != "" || $SESSION != "" ) ]];then
    >&2 echo "ERROR: --stage not specified, but --subject and/or --session are.  We need to know where to look [source|rawdata|derivatives]"
    exit 1
fi
if [[ $STAGE == "source" ]];then    
    ROOT="./projects/$PROJECT/dataset/$STAGE/$SUBJECT/$SESSION/"   
    SOURCE="$DATACOMMONS/$ROOT" ; TARGET="$WORKINGDIR/$ROOT"
    if [[ ! -d $SOURCE ]];then  
        echo "...Tried: $SOURCE"      
        ROOT="./projects/$PROJECT/dataset/$STAGE/$SUBJECT/"
        SOURCE="$DATACOMMONS/$ROOT" ; TARGET="$WORKINGDIR/$ROOT"
        if [[ ! -d $SOURCE ]];then     
            echo "...Tried: $SOURCE"             
            ROOT="./projects/$PROJECT/dataset/$STAGE/sub-$SUBJECT/ses-$SESSION/"
            SOURCE="$DATACOMMONS/$ROOT" ; TARGET="$WORKINGDIR/$ROOT"
            if [[ ! -d $SOURCE ]];then   
                echo "...Tried: $SOURCE"                   
                ROOT="./projects/$PROJECT/dataset/$STAGE/sub-$SUBJECT/" 
                SOURCE="$DATACOMMONS/$ROOT" ; TARGET="$WORKINGDIR/$ROOT"                   
                    if [[ ! -d $SOURCE ]];then  
                        echo "...Tried: $SOURCE"                                                    
                        >&2 echo "ERROR: Unable to locate matching source on data commons"    
                        exit 1                    
                    fi
                    
            fi
        fi
    fi
    echo "Found matching source at $SOURCE"
    echo "This will be cloned to $TARGET"
fi
mkdir -p $TARGET
if [[ $? -ne 0 ]];then
    >&2 echo "ERROR: Unable to create target directory $TARGET"
    exit 1
fi
seconds=0
rsync -r $SOURCE $TARGET
echo "rsync ran for $SECONDS seconds"


