#!/bin/bash

#if [[ -d /tmp/air-neuro ]];then
# rm -r /opt/local/air-neuro
#fi
#mkdir -p /opt/local/air-neuro
#cp -r scripts /opt/local/air-neuro

sudo singularity build  air-neuro.sif /home/vagrant/ac-client/containers/air-neuro/air-neuro.def 

if [[ $? -eq 0 ]];then
sudo mv air-neuro.sif /home/vagrant/singularity-containers
echo run this:
echo singularity push -U /home/vagrant/singularity-containers/air-neuro.sif library://dmattie/default/image 
echo then update tasks
fi


