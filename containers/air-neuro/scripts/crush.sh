#!/bin/bash


#set -e #Exit when a command fails
set +x #echo each command before it runs
echo "Crushing..."
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"
source "${SCRIPTPATH}/lib/crush/crush_import.sh"

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Cartesian product of Region to Region tract measurement extraction"
   echo
   echo "Usage: crush [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--datasetdir  DIR                      Path to dataset directory (just above ../[source|rawdata|derivatives]/..)"
   echo "--subject SUBJECT                      Specify subject ID to clone.  If session not"
   echo "                                       specified, then clone the entire subject"
   echo "--session SESSION                      Specify session ID to clone"  
   echo "--pipeline PIPELINE_ID                 Specify derivatives directory to store output.  If unspecified, store in crush." 
   echo "--gradientmatrix GRADIENTMATRIX        If a gradient matrix file has been provided, specifiy its location here"
   echo "--maxcores MAX                         Specify a hard limit on the number of cores used"
   echo "--overwrite                            Overwrite any existing derivative files with a conflicting name"
   echo "--overlay                              Path to optional singularity overlay file"
   echo "--verbose                              Print out all commands executed"
   
   echo
}


############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options

TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:,pipeline:,maxcores:,gradientmatrix:,overwrite,verbose,overlay:\
             -n 'crush' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

DATASETDIR=""
SUBJECT=""
SESSION=""
PIPELINE=""
GRADIENTMATRIX=""
MAXCORES=""
OVERWRITE=0
VERBOSE="N"
OVERLAY=""

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datasetdir ) DATASETDIR="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;
    --pipeline ) PIPELINE="$2";shift 2;;
    --gradientmatrix ) GRADIENTMATRIX="$2";shift 2;;
    --maxcores ) MAXCORES="$2";shift 2;;
    --overwrite ) OVERWRITE=1;shift;; 
    --overlay ) OVERLAY="$2";shift 2;;    
    --verbose ) VERBOSE="Y";shift;;                
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ $VERBOSE == "Y" ]];then
    set -x
fi
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

if [[ $PIPELINE == "" ]];then
    >&2 echo "ERROR: pipeline not specified"
    exit 1
fi
if [[ $SESSION == "" ]];then
    >&2 echo "WARNING: session not specified"  
    SESSIONpath=""
else
    SESSIONpath="ses-$SESSION"  
fi

if [[ $GRADIENTMATRIX != "" && ! -f $GRADIENTMATRIX ]];then
    >&2 echo "ERROR: A gradient matrix has been specified but cannot be found ($GRADIENTMATRIX)"
    exit 1    
fi  


SOURCE=$DATASETDIR/rawdata/sub-$SUBJECT/$SESSIONpath
TARGET=$DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath
FREESURFER=$DATASETDIR/derivatives/freesurfer/sub-$SUBJECT/$SESSIONpath
  
if [[ ! -d $SOURCE ]];then
    >&2 echo "ERROR: Specified source directory doesn't exist: ($SOURCE)"
    exit 1
fi

echo "Cleaning house..."
if [[ $OVERWRITE -eq 1 ]];then

    rm -r --force $TARGET/crush  #Clean up old crush derived results
    rm --force $TARGET/core.* #Remove old core dumps

    echo "Any previous output have been removed"
fi

mkdir -p $TARGET

if [[ $GRADIENTMATRIX != "" ]];then
    cp $GRADIENTMATRIX $TARGET/gradientmatrix_dti.txt
    cp $GRADIENTMATRIX $TARGET/gradientmatrix_qball.txt
fi


#####################################
#  ROI x ROI measurement extraction #
#####################################

if [[ -f $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath/parcellations/wmparc-parcellated.tar ]];then
   cd $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath/parcellations
   tar -xf wmparc-parcellated.tar
fi
echo "Checking overlay..."
if [[ -z "${APPTAINER_NAME}" ]]; then
  if [[ $OVERWRITE -eq 1 ]];then
    rm -r --force /crush
  fi
  mkdir -p /crush
  if [[ $? -eq 0 ]];then
    OVERLAY_PATH="/crush" 
    echo "You appear to have an overlay file. Crush will work in $OVERLAY_PATH"
  else    
    OVERLAY_PATH=""
    echo "No overlay file detected.  It is strongly encouraged to use an overlay file to improve performance and avoid disk quotas.  See APPTAINER overlays."
  fi  
else
  OVERLAY_PATH=""
fi
echo "Crushing..."

if [[ $MAXCORES == "" ]];then
    python3 ${SCRIPTPATH}/lib/crush/crush.py -datasetdir $DATASETDIR \
    -subject $SUBJECT \
    -session "$SESSION" \
    -pipeline $PIPELINE \
    -overlay "$OVERLAY_PATH"
else
    python3 ${SCRIPTPATH}/lib/crush/crush.py -datasetdir $DATASETDIR \
    -subject $SUBJECT \
    -session "$SESSION" \
    -pipeline $PIPELINE \
    -maxcores $MAXCORES \
    -overlay "$OVERLAY_PATH"
fi

if [[ -f $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath/parcellations/wmparc-parcellated.tar ]];then
   cd $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath/parcellations
   rm *.nii
fi

echo "Consolidating..."
python3 ${SCRIPTPATH}/lib/crush/consolidate-measurements.py \
-datasetdir $DATASETDIR \
-subject $SUBJECT \
-session "$SESSION" \
-pipeline $PIPELINE \
-out $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath/crush.txt

if [[ $? -eq 0  ]];then
    cd  $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/$SESSIONpath
    if [[ -d crush ]];then
        tar -rf crush.tar crush --remove-files 
    fi
fi
