#!/bin/bash

if [ $# -eq 0 ]
  then
      echo "No arguments supplied."
      echo "USAGE: parcellate.sh workingdirectory subject session"
      echo "    eg: parcellate.sh . 12345 2"
      echo "        this will parcellate ./sub-12345/ses-2/Freesurfer/mri/wmparc.mgz"
      exit
fi
workingdir=$1
UUID=$(cat /proc/sys/kernel/random/uuid)
stagingdir=$AIRCRUSH_SCRATCH/$UUID
subject=$2
session=$3
curdir=$(pwd)
start=`date +%s`

test_canproceed(){
    if [ -d $workingdir ];then
        if [ -d $workingdir/sub-$subject ];then
            if [ -d $workingdir/sub-$subject/ses-$session ];then
                if [ -d $workingdir/sub-$subject/ses-$session/Freesurfer/mri ];then
                    if [ -f  $workingdir/sub-$subject/ses-$session/Freesurfer/mri/wmparc.mgz ];then                    
                        return 0
                    else
                        echo "Reconstructed T1 not found in $workingdir/sub-$subject/ses-$session/Freesurfer.  Sought mri/wmparc.mgz"
                    fi
                    
                else
                    echo "Freesurfer/mri directory not found in $workingdir/sub-$subject/ses-$session/Freesurfer/mri"
                fi
            else
                echo "Session (ses-$subject) not found: $workingdir/sub-$subject/ses-$session"
            fi

        else
            echo "Subject (sub-$subject) not found in working directory"
        fi
    else
        echo "Working directory ($workingdir) not found"
    fi
}
test_complete() {

    missing_segments=0
    rois=$( cat ../assets/segmentMap.csv |grep -v "^#"|cut -d\, -f 1|tr "\n" ';' )    
    IFS=";" read -ra roiarray <<< "$rois"

    
    for roi in "${roiarray[@]}"
    do
        if [ ! -f $workingdir/sub-$subject/ses-$session/Tractography/parcellations/wmparc$roi.nii ];then            
            missing_segments=$((missing_segments+1))
        fi           
    done
    
    if [ $missing_segments -gt 0 ];then
        echo "$missing_segments ROIs not yet segmented"
        return 1
    else
        echo "Parcellation is complete"
        return 0
    fi
}

parcellate() {    
    wd=$1
    subject=$2
    session=$3
    mkdir -p $stagingdir/sub-$subject/ses-$session/Tractography/parcellations

    rois=$( cat ../assets/segmentMap.csv |grep -v "^#"|cut -d\, -f 1|tr "\n" ';' )    
    IFS=";" read -ra roiarray <<< "$rois"

    
    for roi in "${roiarray[@]}"
    do
        touch $stagingdir/sub-$subject/ses-$session/Tractography/parcellations/wmparc$roi.nii             
    done  

    rsync -r $stagingdir/sub-$subject/ses-$session/ $workingdir/sub-$subject/ses-$session
    if [ $? == 0 ];then
        rm -r $stagingdir
        echo "Return staging to working directory complete."
    else
        echo "ERROR: Failed to sync staging area ($stagingdir) back to working directory ($workingdir).  Parcellation is incomplete."
        return 2
    fi    

}

if ! test_complete;then  

    if test_canproceed;then
        __dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        source ${__dir}/stage.sh $workingdir $subject $session $stagingdir
        if [ $? == 0 ];then
            parcellate $workingdir $subject $session
            if [ $? != 0 ];then
                echo "ERROR: Parcellation failed.  See error log for details."
            else

                end=`date +%s`
                runtime=$((end-start))
                echo "Parcellation complete. Runtime: $runtime"
            fi
        else
            echo "ERROR: Unable to establish a copy of working directory to ensure atomicity"
            exit 2
        fi
        
    else
        echo "Unable to proceed. Exiting"    
    fi
fi