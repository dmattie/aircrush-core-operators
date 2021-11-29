#!/bin/bash

############################################################
# dti_recon                                                #
############################################################
dti_recon()
{
  matrix=$1

    if [[ -f $matrix ]];then
        echo "Matrix file already exists. I will use it."
        exit 0
    fi