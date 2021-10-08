#!/bin/bash
SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/helper.sh"

#simulate the conversion of source data to BIDS standard
SOURCE=$1
TARGET=$2


subject=$( get_subject $SOURCE )
session=$( get_session $SOURCE )

base_subject=$( strip_prefix $subject )
base_session=$( strip_prefix $session )


cd $TARGET

mkdir -p sub-${base_subject}/ses-${session}/anat
mkdir -p sub-${base_subject}/ses-${session}/dwi

touch sub-${base_subject}/ses-${base_session}/anat/anat.nii.gz
touch sub-${base_subject}/ses-${base_session}/dwi/dti.nii.gz
touch sub-${base_subject}/ses-${base_session}/dwi/bvals
touch sub-${base_subject}/ses-${base_session}/dwi/bvecs

