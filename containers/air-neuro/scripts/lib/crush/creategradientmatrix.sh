#!/bin/bash

############################################################
# creategradientmatrix                                     #
############################################################
f_creategradientmatrix()
{
    matrix=$1
    imaging_model=$2

    if [[ $GRADIENTMATRIX != "" && -f $GRADIENTMATRIX ]];then
        cp $GRADIENTMATRIX $matrix
        if [[ ! -f $matrix ]];then
            >&2 echo "Unable to copy specified gradient matrix to $matrix"                        
            return 1
        fi
    else
        bvecs=$DATASETDIR/rawdata/sub-$SUBJECT/ses-$SESSION/dwi/bvecs
        if [[ ! -f $bvecs ]];then
            >&2 echo "Gradient table not specified and convertable bvecs not found ($bvecs).  Unable to proceed."
            
            return 1
        fi   

        ${SCRIPTPATH}/lib/crush/create_gradient_matrix.py -bvec $bvecs -out $matrix -imaging_model $imaging_model

        if [[ ! -f $matrix ]];then
            >&2 echo "Gradient table ($matrix) could not be created from ($bvecs).  Unable to proceed."
            return 1
        fi

    fi
    return 0

}
export f_creategradientmatrix