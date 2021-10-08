#!/bin/bash

if [ $# -eq 0 ]
  then
      echo "No arguments supplied."
      echo "USAGE: recon.sh workingdirectory subject session"
      echo "    eg: recon.sg . 12345 2"
      echo "        this will reconstruct ./sub-12345/ses-2/"
      exit
fi

if [[ -z "${AIRCRUSH_SCRATCH}" ]];then
    echo "Environment variable AIRCRUSH_SCRATCH not set. It must be set to continue"
    exit 1
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
                if [ -d $workingdir/sub-$subject/ses-$session/original/anat ];then
                    infile=$( ls $workingdir/sub-$subject/ses-$session/original/anat/sub-${subject}_ses-${session}*.nii )
                    if [ ! $infile == "" ];then
                        return 0
                    else
                        echo "T1 .nii not found in $workingdir/sub-$subject/ses-$session/original/anat"
                        return 5
                    fi
                    
                else
                    echo "anat directory not found in $workingdir/sub-$subject/ses-$session/original/anat"
                    return 4
                fi
            else
                echo "Session (ses-$subject) not found: $workingdir/sub-$subject/ses-$session/original/anat"
                return 3
            fi

        else
            echo "Subject (sub-$subject) not found in working directory"
            return 2
        fi
    else
        echo "Working directory ($workingdir) not found"
        return 1
    fi
}
test_complete() {
    
    if [[ -f $workingdir/sub-$subject/ses-$session/Freesurfer/mri/brainmask.mgz && -f $workingdir/sub-$subject/ses-$session/Freesurfer/mri/wmparc.mgz ]];then
        echo "Reconstruction completed.  Found $workingdir/sub-$subject/ses-$session/Freesurfer/mri/wmparc.mgz, brainmask.mgz"
        return 0
    else
        return 1
    fi
}

recon() {    

    mkdir -p $stagingdir/sub-$subject/ses-$session/Freesurfer/mri
    touch $stagingdir/sub-$subject/ses-$session/Freesurfer/mri/wmparc.mgz
    touch $stagingdir/sub-$subject/ses-$session/Freesurfer/mri/brainmask.mgz

    echo "Recon Complete."
    rsync -r $stagingdir/sub-$subject/ses-$session/ $workingdir/sub-$subject/ses-$session
    if [ $? == 0 ];then
        rm -r $stagingdir
        echo "Return staging to working directory complete."
    else
        echo "ERROR: Failed to sync staging area ($stagingdir) back to working directory ($workingdir).  Recon is incomplete"
        return 2
    fi    
}

if ! test_complete;then  

    if test_canproceed;then        
        __dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        source ${__dir}/stage.sh $workingdir $subject $session $stagingdir
        if [ $? == 0 ];then
            ######################################<<<<<<<<<<<<<<<<<<
            recon
            ######################################<<<<<<<<<<<<<<<<<<
            if [ $? != 0 ];then
                echo "ERROR: Reconstruction failed.  See error log for details."
                exit 3
            else

                end=`date +%s`
                runtime=$((end-start))
                echo "Recon complete. Runtime: $runtime"
            fi
        else
            echo "ERROR: Unable to establish a copy of working directory to ensure atomicity"
            exit 2
        fi
            
    else
        echo "Unable to proceed. Exiting" 
        exit 1   
    fi
fi