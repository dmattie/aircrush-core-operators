#!/bin/bash

sudo singularity build --sandbox air-neuro.sif air-neuro.def 

if [[ $? -eq 0 ]];then
#:sudo mv air-neuro.sif ~/singularity-containers
echo run this:
echo singularity push -U /home/vagrant/singularity-containers/air-neuro.sif library://dmattie/default/image 
echo then update tasks
fi


