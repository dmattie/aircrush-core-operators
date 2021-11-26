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

function case_a_autobids {  ##ABIDE LOOK-A-LIKE  
  mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat  
  cp $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1/anat.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat/sub-${SUBJECT}_ses-${SESSION}_anat.nii.gz
  if [[ -d $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1 ]];then
    mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1/dti.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/sub-${SUBJECT}_ses-${SESSION}_dwi.nii.gz
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvals $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvals
    cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvecs_image $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvecs
  fi
  echo "OK"
}

export case_a_test
export case_a_autobids
