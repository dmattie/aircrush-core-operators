#!/bin/bash

if [ $# -eq 0 ]
  then
      echo "No arguments supplied."
      echo "USAGE: registration.sh workingdirectory subject session"
      echo "    eg: registration.sh . 12345 2"
      echo "        this will register dwi to T1 and product ./sub-12345/ses-2/Tractography/reg2brain.data.nii"
      exit
fi
workingdir=$1
subject=$2
session=$3
UUID=$(cat /proc/sys/kernel/random/uuid)
stagingdir=$AIRCRUSH_SCRATCH/$UUID
curdir=$(pwd)
start=`date +%s`

test_canproceed(){
    #must have a reconstructed brainmask in Freesurfer/mri and a diffusion volume (.nii) in original/dwi
    if [ -d $workingdir ];then
        if [ -d $workingdir/sub-$subject ];then
            if [ -d $workingdir/sub-$subject/ses-$session ];then

                if [ -d $workingdir/sub-$subject/ses-$session/Freesurfer/mri ];then
                    if [ ! -f  $workingdir/sub-$subject/ses-$session/Freesurfer/mri/brainmask.mgz ];then                                                            
                        echo "ERROR: Reconstructed T1 not found in $workingdir/sub-$subject/ses-$session/Freesurfer.  Sought mri/brainmask.mgz"
                        return 7
                    fi                    
                else
                    echo "ERROR: Freesurfer/mri directory not found here: $workingdir/sub-$subject/ses-$session/Freesurfer/mri"
                    return 6
                fi

                if [ -d $workingdir/sub-$subject/ses-$session/original/dwi ];then
                    infile=$( ls $workingdir/sub-$subject/ses-$session/original/dwi/*.nii )
                    if [ $? == 0 ];then
                        if [[ $? == 0 &&  -f  $infile.nii ]];then                                                                
                            echo "ERROR: Diffusion volume not found in $workingdir/sub-$subject/ses-$session/original/dwi.  Sought first *.nii"
                            return 5
                        fi   
                    else
                        echo "ERROR: There are no .nii files found in $workingdir/sub-$subject/ses-$session/original/dwi"
                        return 8
                    fi                 
                else
                    echo "ERROR: original/dwi directory not found in $workingdir/sub-$subject/ses-$session/original/dwi"
                    return 4
                fi                

            else
                echo "ERROR: Session (ses-$subject) not found: $workingdir/sub-$subject/ses-$session"
                return 3
            fi

        else
            echo "ERROR: Subject (sub-$subject) not found in working directory"
            return 2
        fi
    else
        echo "ERROR: Working directory ($workingdir) not found"
        return 1
    fi
}
test_complete() {

    if [ -f $workingdir/sub-$subject/ses-$session/Tractography/reg2brain.data.nii.gz ];then
        echo "INFO: Registered diffusion volume detected: ($workingdir/sub-$subject/ses-$session/Tractography/reg2brain.data.nii.gz)"
        return 0
    else
        echo "WARN: Registered diffusion volume does not yet exist ($workingdir/sub-$subject/ses-$session/Tractography/reg2brain.data.nii.gz)"
        return 1
    fi
}

registration() {    
    touch $stagingdir/sub-$subject/ses-$session/Tractography/reg2brain.data.nii.gz 

    rsync -r $stagingdir/sub-$subject/ses-$session/ $workingdir/sub-$subject/ses-$session
    if [ $? == 0 ];then
        rm -r $stagingdir
        echo "INFO: Return staging to working directory complete."
    else
        echo "ERROR: Failed to sync staging area ($stagingdir) back to working directory ($workingdir).  Registration is incomplete."
        return 2
    fi
}

if ! test_complete;then  

    if test_canproceed;then
        
        __dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        source ${__dir}/stage.sh $workingdir $subject $session $stagingdir
        if [ $? == 0 ];then

            registration
            if [ $? != 0 ];then
                echo "ERROR: Registration failed.  See error log for details."
            else
                

                end=`date +%s`
                runtime=$((end-start))
                echo "INFO: Registration complete. Runtime: $runtime"
                if ! test_complete;then
                    exit 3
                fi
            fi
        else
            echo "ERROR: Unable to establish a copy of working directory to ensure atomicity"
            exit 2
        fi    
    else
        echo "ERROR: Unable to perform registration of diffusion volume to T1. Exiting"  
        exit 1  
    fi
fi