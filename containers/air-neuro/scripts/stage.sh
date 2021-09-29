#!/bin/bash

if [ $# -eq 0 ]
  then
      echo "No arguments supplied."
      echo "USAGE: stage.sh workingdirectory subject session UUID"
      echo "    eg: stage.sh . 12345 2 /tmp/12345-12312-12323-12312"
      echo "        this will rsync -r ./sub-12345/ses-2/ to /tmp/12345-12312-12323-12312"
      exit
fi
workingdir=$1
subject=$2
session=$3
stagingdir=$4
start=`date +%s`

echo "copy working directory ($workingdir/sub-$subject/ses-$session) to staging area ($AIRCRUSH_SCRATCH/$uid/sub-$subject/ses-$session) to ensure atomicity"
mkdir -p $stagingdir/sub-$subject/ses-$session
rsync -r $workingdir/sub-$subject/ses-$session/ $stagingdir/sub-$subject/ses-$session
