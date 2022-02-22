#!/bin/bash


############################################
# CASE B ADNI
############################################
function case_b_test {  ## ADNI-ii   
  
  if [ -d $DATASETDIR/source/$SUBJECT/ ] && 
    (
       [ -d $DATASETDIR/source/$SUBJECT/MPRAGE ] || 
       [ -d $DATASETDIR/source/$SUBJECT/Axial_DTI ] ||
       [ -d $DATASETDIR/source/$SUBJECT/Accelerated_Sagittal_MPRAGE ] 
    )
  then
    echo "TRUE"
  else  
    echo "FALSE"
  fi
  
}
function case_b_autobids {  ##ADNI-ii LOOK-A-LIKE 
  #Let's do T1 first 
  shopt -s globstar
  anat_processed=0

  #There may be an xml file with metadata. Look here first
  for eachdir in $DATASETDIR/source/$SUBJECT/*MPRAGE/**/*.xml; do
    case_b_process_mprage $eachdir
    anat_processed=1
  done

  if [[ $anat_processed -eq 0 ]];then
    #No xml file found, let's try our best:
    for eachdir in $DATASETDIR/source/$SUBJECT/*MPRAGE/**/*_1_*.dcm; do
      #each represents first of a series in dcm set
      case_b_noxml_process_mprage $eachdir
    done
  fi

  for eachdcm in $DATASETDIR/source/$SUBJECT/*DTI*/**/*_1_*.dcm; do
    #each represents first of a series in dcm set
    case_b_process_dti $eachdcm    
  done
#   mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat  
#   cp $DATASETDIR/source/$SUBJECT/session_$SESSION/anat_1/anat.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/anat/sub-${SUBJECT}_ses-${SESSION}_anat.nii.gz
#   if [[ -d $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1 ]];then
#     mkdir -p $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi
#     cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_1/dti.nii.gz $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/sub-${SUBJECT}_ses-${SESSION}_dwi.nii.gz
#     cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvals $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvals
#     cp $DATASETDIR/source/$SUBJECT/session_$SESSION/dti_fieldmap/dti.bvecs_image $TARGET/sub-${SUBJECT}/ses-${SESSION}/dwi/bvecs
#   fi
  echo "OK"  
}
function case_b_process_mprage {

    sessdir=$( dirname $1 )
    session=$( cat $1 |grep series|cut -d\" -f2 )    
    image=$( cat $1 |grep image|cut -d\" -f2 )  
    nii=$(find $sessdir -name *.nii -print -quit 2>/dev/null | head -1)
    newsub=$(echo $SUBJECT | tr -cd '[:alnum:]')
    mkdir -p $DATASETDIR/rawdata/sub-${newsub}/ses-${session}/anat
    cp --no-clobber $1 $DATASETDIR/rawdata/sub-${newsub}/ses-${session}/anat/sub-${newsub}_ses-${session}_T1_$image.nii
}
function case_b_noxml_process_mprage {
    sessdir=$( dirname $1 )
    image=$( echo $1 |rev|cut -d\_ -f1 |rev|cut -d\. -f1)
    session=$( echo $1 |rev|cut -d\_ -f2 |rev)
    newsub=$(echo $SUBJECT | tr -cd '[:alnum:]')    
    mkdir -p $DATASETDIR/rawdata/sub-${newsub}/ses-${session}/anat
    cp --no-clobber $1 $DATASETDIR/rawdata/sub-${newsub}/ses-${session}/anat/sub-${newsub}_ses-${session}_T1_$image.nii
}
function case_b_process_dti {
    sessdir=$( dirname $1 )
    
    image=$( echo $1 |rev|cut -d\_ -f1 |rev|cut -d\. -f1)      
    newsub=$(echo $SUBJECT | tr -cd '[:alnum:]')
    target="$DATASETDIR/rawdata/sub-${newsub}/ses-${session}/dwi/"
    mkdir -p $target
    rsync -r $sessdir/ $target

    if [ -f $target/*.json ];then 
        rm $d/*.json
    fi
    if [ -f $target/*.nii ];then
        rm $d/*.nii
    fi
    if [ -f $target/*.bvec ];then
        rm $d/*.bvec
    fi
    if [ -f $target/*.bval ];then
        rm $d/*.bval
    fi

    dcm2niix $target
    
    #cp --no-clobber $1 $DATASETDIR/rawdata/sub-${newsub}/ses-${session}/anat/sub-${newsub}_ses-${session}_T1_$image.nii
}

export case_a_test
export case_b_autobids