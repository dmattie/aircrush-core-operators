#!/bin/bash

if [[ -d /tmp/air-neuro ]];then
 rm -r /tmp/air-neuro
fi
mkdir -p /tmp/air-neuro
cp -r scripts /tmp/air-neuro

cd /media/dmattie/GENERAL/singularity-containers
sudo singularity build  air-neuro.sif ~/projects/ac-core/containers/air-neuro/air-neuro.def 


