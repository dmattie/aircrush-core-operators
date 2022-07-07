#!/bin/bash

project=$1
subject=$2
session=$3
remote=$4

echo "Re-committing $project/${subject}/${session}"

fn="sub-${subject}/sub-${subject}_ses-${session}.tar"
source="/scratch/dmattie/datacommons/projects/$project/datasets/derivatives/"
dest="/home/dmattie/projects/rrg-jlevman/shared/projects/$project/datasets/derivatives/"

for derivative in "levman" "freesurfer"
do
  src="$source/$derivative/$fn"
  if [[ -f $src ]];then
      echo "syncing $src"
      if [[ $remote == "" ]];then
        remotesuffix=""
        mkdir -p $dest/$derivative/sub-${subject}
      else 
        remotesuffix=":"
        ssh $remote mkdir -p $dest/$derivative/sub-${subject}
	  if [[ ! $? -eq 0 ]];then
	    echo "Failed to make target directory"
	    exit 1
	  fi
          rsync $source/$derivative/$fn ${remote}${remotesuffix}${dest}/${derivative}/${fn}
	  if [[ ! $? -eq 0 ]];then
            echo "rsync failed"
	    exit 1
	  else
	    mv $source/$derivative/$fn $source/$derivative/$fn.deleteme
	  fi
	  find "$source/$derivative/sub-$subject" -maxdepth 0 -empty -exec rmdir {} \; #delete if empty
      fi
  else
     echo "Source not found ( $derivative/$subject/$session ) Skipping.."
  fi
done
