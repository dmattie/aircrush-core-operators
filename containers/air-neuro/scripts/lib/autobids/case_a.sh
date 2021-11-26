#!/bin.bash

############################################
# CASE A
############################################
function case_a_test {  ## ABIDE    
  if [[ -d $DATASETDIR/source/$SUBJECT 
    && -d $DATASETDIR/source/$SUBJECT/session_$SESSION
    && -d $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1 
    && -f $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1/anat.nii.gz ]]
  then
    echo "TRUE"
  else
    echo "FALSE"
  fi
}

# function case_a_autobids {  ##ABIDE LOOK-A-LIKE  
#   mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat  
#   cp $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1/anat.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat/sub-${SUBJECT}_ses-${SESSION}_anat.nii.gz
#   if [[ -d $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1 ]];then
#     mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi
#     cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1/dti.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/sub-${SUBJECT}_ses-${SESSION}_dwi.nii.gz
#     cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvals $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvals
#     cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvecs_image $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvecs
#   fi
#   echo "OK"
# }
function case_a_autobids {  ##ABIDE LOOK-A-LIKE  
  echo "============ HERE ========="
  shopt -s globstar
  echo "Process ANAT"
  for eachdir in $DATASETDIR/source/$SUBJECT/session_$SESSION/anat*;do
    if [[ -d $eachdir ]];then        
        process_anat $eachdir
    fi
  done  
  echo "Process DTI"
  for eachdir in $DATASETDIR/source/$SUBJECT/session_$SESSION/dti*;do
    if [[ -d $eachdir ]];then
        echo $eachdir
        process_dti $eachdir
    fi
  done    
  echo "OK"
}
function process_anat {
    anatdir=$( dirname $1 )
    SCANID=$( echo $anatdir |rev|cut -d\_ -f1 |rev )
    mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat  
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_${SCANID}/anat.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat/sub-${SUBJECT}_ses-${SESSION}_anat_${SCANID}.nii.gz

}
function process_dti {

    dtidir=$( dirname $1 )
    SCANID=$( echo $dtidir |rev|cut -d\_ -f1 |rev )
    mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi
    cp $dtidir/dti.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/sub-${SUBJECT}_ses-${SESSION}_dwi_${SCANID}.nii.gz
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvals $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvals
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvecs_image $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvecs

    
}
export case_a_test
export case_a_autobids
