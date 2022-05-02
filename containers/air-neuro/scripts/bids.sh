#!/bin/bash
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"
source "${SCRIPTPATH}/lib/autobids.sh"

#set -e #Exit when a command fails
#set -x #echo each command before it runs

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
    >&2 echo "ERROR: dataset directory specified not found ($DATASETDIR)"
    exit 1
fi
if [[ $SUBJECT == "" ]];then
    >&2 echo "ERROR: subject not specified"
    exit 1
fi

TARGET=$DATASETDIR/rawdata/

#subject=$( get_subject $SOURCE )

if [[ $( case_a_test ) == "TRUE" ]];then
  echo "It looks like source matches pattern 'A'. Applying autobids rules for pattern 'A'"
  res=$( case_a_autobids )
  if [[ $res == "OK" ]];then
    exit 0
  else      
    echo "[ERROR] Autbids Pattern A attempted but failed"
    echo $res
  fi
else
  echo "FALSE"
fi

if [[ $( case_b_test ) == "TRUE" ]];then
  echo "It looks like source matches pattern 'B'. Applying autobids rules for pattern 'B'"
  res=$( case_b_autobids )
  if [[ $res == "OK" ]];then
    exit 0
  else
    echo "[ERROR] Autobids Pattern B attempted but failed"
    echo $res
  fi
fi


echo "[WARNING] Source does not match any convertible pattern.  Manual conversion to BIDS needed. Unable to continue"
exit 1
