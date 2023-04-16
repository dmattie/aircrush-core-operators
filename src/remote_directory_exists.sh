#!/bin/bash

#set -e #Exit when a command fails
set +x #echo each command before it runs

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "List Derivatives"
   echo
   echo "Usage: list_derivatives.sh [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--commons_path PATH                    Specify remote directory of data commons root." 
   echo "--data_transfer_node HOST              Specify cluster where the data commons is stored"
   echo "--project PROJECTID                    Project ID of subject"     
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"  
   echo "--verbose                              Print out all commands executed"
   
   echo
}


############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options

TEMP=`getopt -o h: --long help,commons-path:,subject:,session:,project:,data-transfer-node:,verbose,\
             -n 'list_derivatives' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

COMMONS_PATH=""
SUBJECT=""
SESSION=""
PROJECT=""
DATA_TRANSFER_NODE=""
VERBOSE="N"

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --commons-path ) COMMONS_PATH="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
    --project ) PROJECT="$2";shift 2;;
    --data-transfer-node ) DATA_TRANSFER_NODE="$2";shift 2;; 
    --verbose ) VERBOSE="Y";shift;;                
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ $VERBOSE == "Y" ]];then
    set -x
fi

if [ ssh $DATA_TRANSFER_NODE ls $COMMONS_PATH ]
then
{
    echo 'hi'
}
fi