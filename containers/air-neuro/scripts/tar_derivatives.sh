#!/bin/bash


#set -e #Exit when a command fails
#set +x #echo each command before it runs

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )

OKGREEN='\033[92m'
OKCYAN='\033[96m'
WARNING='\033[93m'
FAIL='\033[91m'
ENDC='\033[0m'

# source "${SCRIPTPATH}/lib/helper.sh"
# source "${SCRIPTPATH}/lib/crush/crush_import.sh"

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Tar all subject/[session] derivatives"
   echo
   echo "Usage: crush_wrapup [OPTIONS...]"
   echo "options:"
   echo "--help                                 Print this Help."
   echo "--datasetdir  DIR                      Path to dataset directory (just above ../[source|rawdata|derivatives]/..)"
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

TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:\
             -n 'crush_wrapup' -- "$@"`


if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
#eval set -- "$TEMP"

DATASETDIR=""
SUBJECT=""
SESSION=""
VERBOSE="N"

while true; do
  case "$1" in
    -h | --help ) Help;exit 1;;
    --datasetdir ) DATASETDIR="$2";shift 2;;     
    --subject ) SUBJECT="$2";shift 2;;
    --session ) SESSION="$2";shift 2;;    
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


if [[ $SESSION == "" ]];then
    >&2 echo "WARNING: session not specified"  
    SESSIONpath=""
    SESSIONseparator=""
else
    SESSIONpath="ses-$SESSION"  
    SESSIONseparator="_"
fi

if [[ $GRADIENTMATRIX != "" && ! -f $GRADIENTMATRIX ]];then
    >&2 echo "ERROR: A gradient matrix has been specified but cannot be found ($GRADIENTMATRIX)"
    exit 1    
fi  



TARGET=$DATASETDIR/derivatives/levman/sub-$SUBJECT/$SESSIONpath
FREESURFER=$DATASETDIR/derivatives/freesurfer/sub-$SUBJECT/$SESSIONpath
  
DERIVATIVES=( $TARGET $FREESURFER )
for derivative in "${DERIVATIVES[@]}"
do
    echo -e "${OKGREEN}TARRING...${ENDC}${derivative}"
    parent=`dirname $derivative`
    if [[ ! -d $parent ]];then
        >&2 echo "ERROR: Specified directory doesn't exist: ($parent).  Dataset may be incomplete"
        exit 1
    else
        cd $parent      
        if [[ $SESSIONpath == "" ]];then
            echo -e "$WARNING No session passed $ENDC"
            if [[ -d sub-$SUBJECT ]];then
                
                if [[ -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar ]];then
                    echo -e "${WARNING}TAR already exists but WILL be overwritten because the intended directory (sub-$SUBJECT) to tar also exists.  $parent/sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar{$ENDC}"
                    rm --force sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar 
                fi  
                tar -cf sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar sub-$SUBJECT              
                if [[ $? -eq 0 ]];then   
                    echo "   Done.  Removing subject path $parent/$sub-$SUBJECT"                 
                    rm -r sub-$SUBJECT
                else                
                    if [[ -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar ]];then
                        echo "Failed to create TAR sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar.  It has been removed automatically"
                        rm sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar                
                    fi
                    echo "TAR command returned an error.  Previous messages should elucidate."
                fi
            else
                if [[ -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar ]];then
                    echo -e "${OKCYAN}TAR already exists and will not be overwritten${ENDC} $parent/sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar because the orginal directory no longer exists."
                else
                    echo "Nothing to tar.  Sought ${derivative}"
                    exit 1
                fi
            fi
        else
            if [[ -d $SESSIONpath ]];then
                
                if [[ -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar ]];then
                    echo -e "${WARNING}TAR already exists but WILL be overwritten because the intended directory to tar ($SESSIONPath) also exists.  $parent/sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar${ENDC}"
                    rm -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar 
                fi
                tar -cf sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar $SESSIONpath
                if [[ $? -eq 0 ]];then
                    echo "   Done.  Removing session path $parent/$SESSIONpath"
                    rm -r $SESSIONpath
                else                
                    if [[ -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar ]];then
                        echo "Failed to create TAR sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar.  It has been removed automatically"
                        rm sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar
                    fi
                    echo "TAR command returned an error.  Previous messages should elucidate."
                    exit 1                
                fi            
            else   
                if [[ -f sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar ]];then
                    echo -e "${OKCYAN}TAR already exists and will NOT be overwritten${ENDC} ($parent/sub-${SUBJECT}${SESSIONseparator}${SESSIONpath}.tar) because the orginal directory no longer exists."
                else 
                    echo "Nothing to tar.  Sought ${derivative}"
                    exit 1
                fi
            fi
        fi
    fi

done



