#!/bin/bash


#set -e #Exit when a command fails
#set -x #echo each command before it runs

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
   echo "--overwrite                            Overwrite any existing derivative files with a conflicting name"
   echo "--invert_x                             [dti|odf]_tracker switch to invert x vector"
   echo "--invert_y                             [dti|odf]_tracker switch to invert y vector"
   echo "--invert_z                             [dti|odf]_tracker switch to invert x,y, or z component(s) of vector"
   echo "--swap_sxy                             [dti|odf]_tracker switch to swap x and y vectors while tracking"
   echo "--swap_syz                             [dti|odf]_tracker switch to swap y and z vectors while tracking"
   echo "--swap_szx                             [dti|odf]_tracker switch to swap x and z vectors while tracking"
   
   echo
}


############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options

TEMP=`getopt -o h: --long help,datasetdir:,subject:,session:,pipeline:,gradientmatrix:,bmax:,b0:,overwrite,invert_x,invert_y,invert_z,swap_sxy,swap_syz,swap_szx\
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
OVERWRITE=0
INVERT_X=""
INVERT_Y=""
INVERT_Z=""
SWAP_SXY=""
SWAP_SYZ=""
SWAP_SZX=""

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
    --overwrite ) OVERWRITE=1;shift;; 
    --invert_x ) INVERT_X=" -ix";shift;;     
    --invert_y ) INVERT_Y=" -iy";shift;;     
    --invert_z ) INVERT_Z=" -iz";shift;;             
    --swap_sxy ) SWAP_SXY=" -sxy";shift;;         
    --swap_syz ) SWAP_SYZ=" -syz";shift;;         
    --swap_szx ) SWAP_SZX=" -szx";shift;;                 
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
FREESURFER=$DATASETDIR/derivatives/freesurfer/sub-$SUBJECT/ses-$SESSION
  
if [[ ! -d $SOURCE ]];then
    >&2 echo "ERROR: Specified source directory doesn't exist: ($SOURCE)"
    exit 1
fi


if [[ $OVERWRITE -eq 1 ]];then
    rm $TARGET/hardi_mat*.dat 2> /dev/null #Clear hardi_mat output    
    rm $TARGET/recon_out* 2> /dev/null # clear dti_recon output    
    rm $TARGET/tracts.trk  2> /dev/null #clear dti_tracker/odf_tracker output
    rm $TARGET/odf_tracker.log 2> /dev/null 
    rm $TARGET/dti_tracker.log 2> /dev/null
    rm $TARGET/RegTransform4D 2> /dev/null #clear flirt output
    rm $TARGET/crush.trk 2> /dev/null #Clear track_transform output
    rm $TARGET/crush_qball.trk 2> /dev/null #Clear track_transform output
    rm $TARGET/crush_dti.trk 2> /dev/null #Clear track_transform output
    rm $TARGET/gradientmatrix*.txt 2> /dev/null #Clean up old gradient matrix files
    rm -r $TARGET/crush 2>/dev/null #Clean up old crush derived results

    echo "Any previous output have been removed"
fi

mkdir -p $TARGET
if [[ -f $TARGET/gradientmatrix_dti.txt ]];then
    echo "Existing gradientmatrix for dti imaging model detected.  Skipping (re-)creation."    
else 
    echo "Calculating reconstruction matrix from gradient table"
    f_creategradientmatrix $TARGET/gradientmatrix_dti.txt dti
fi


if [[ -f $TARGET/gradientmatrix_qball.txt ]];then
    echo "Existing gradientmatrix for qball imaging model detected.  Skipping (re-)creation."    
else 
    echo "Calculating reconstruction matrix from gradient table"
    f_creategradientmatrix $TARGET/gradientmatrix_qball.txt qball
fi




res=$?
if [[ $res -ne 0 ]];then
    >&2 echo "ERROR: Unable to establish a gradient matrix.  Unable to continue."
fi

###########################
# HARDI_MAT               #
###########################

 f_hardi_mat $TARGET/gradientmatrix_dti.txt "dti" $TARGET/reg2brain.data.nii.gz
  res=$?

 if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to perform hardi_mat.  Unable to continue."
    exit 1
 fi


 f_hardi_mat $TARGET/gradientmatrix_qball.txt "qball" $TARGET/reg2brain.data.nii.gz
 
  res=$?

 if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to perform hardi_mat (for hardi/q-ball reconstruction).  Unable to continue."
    exit 1
 fi
                        

###########################
# RECON                   #
###########################

f_diffusion_recon $INVERT_X $INVERT_Y $INVERT_Z $SWAP_SXY $SWAP_SYZ $SWAP_SZX
res=$?

if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to perform Cortical Reconstruction.  Unable to continue."
    exit 1
fi


###############################
# flirt / affine registration #
###############################

f_flirt
res=$?

if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to perform flirt/affine registration.  Unable to continue."
    exit 1
fi

#####################################
#  ROI x ROI measurement extraction #
#####################################


 rois=$( cat ${SCRIPTPATH}/../assets/segmentMap.csv |grep -v "^#"|cut -d\, -f 1|tr "\n" ';' )    
    IFS=";" read -ra roiarray <<< "$rois"

    
    for roi in "${roiarray[@]}"
    do
        for roi2 in "${roiarray[@]}"
        do
            if [[ $roi != $roi2 ]];then
                if [[ -f $TARGET/crush_qball.trk ]];then
                  CRUSHTRACT="$TARGET/crush_qball.trk"
                else 
                  CRUSHTRACT="$TARGET/crush_dti.trk"
                fi
                if [[ ! -f $TARGET/crush/$roi/calcs-$roi-$roi2-roi.json ]];then     
                    currdate=$( date )       
                    echo "Started at ${currdate}: ${SCRIPTPATH}/lib/crush/get_tract_measurements.py -tract $CRUSHTRACT -pipeline levman -roi_start $roi -roi_end $roi2 -method roi"
                    #sem -j+0 ${SCRIPTPATH}/lib/crush/get_tract_measurements.py -tract $CRUSHTRACT -pipeline levman -roi_start $roi -roi_end $roi2 -method roi
                    ${SCRIPTPATH}/lib/crush/get_tract_measurements.py -tract $CRUSHTRACT -pipeline levman -roi_start $roi -roi_end $roi2 -method roi &
                fi
                if [[ ! -f $TARGET/crush/$roi/calcs-$roi-$roi2-roi_end.json ]];then
                    currdate=$( date )       
                    echo "Started at ${currdate}: ${SCRIPTPATH}/lib/crush/get_tract_measurements.py -tract $CRUSHTRACT -pipeline levman -roi_start $roi -roi_end $roi2 -method roi_end"
                    #sem -j+0 ${SCRIPTPATH}/lib/crush/get_tract_measurements.py -tract $CRUSHTRACT -pipeline levman -roi_start $roi -roi_end $roi2 -method roi_end
                    ${SCRIPTPATH}/lib/crush/get_tract_measurements.py -tract $CRUSHTRACT -pipeline levman -roi_start $roi -roi_end $roi2 -method roi_end &
                fi
            fi            
        done
        echo "Measuring $roi against all other ROIs ======================="
        #sem --wait
        wait
      
    done

#python $CRUSH_PATH/crush.py -samples $SUBJECTS_DIR -patient sub-$patientID -recrush -fixmissing #-gradienttable ~/projects/def-dmattie/crush/plugins/levman/hcp_gradient_table_from_data_dictionary_3T.csv
#pwd

#if [ -f "$SUBJECTS_DIR/sub-$patientID/ses-$sessionID/Tractography/crush/tracts.txt" ]; then