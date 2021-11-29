#!/bin/bash

############################################################
# creategradientmatrix                                     #
############################################################
creategradientmatrix()
{
    matrix=$1

    if [[ $GRADIENTMATRIX <> "" && -f $GRADIENTMATRIX]];then
        cp $GRADIENTMATRIX $matrix
        if [[ ! -f $matrix ]];then
            echo "Unable to copy specified gradient matrix to $matrix"
            exit 1
        fi
    else
        bvecs=$DATASETDIR/rawdata/sub-$SUBJECT/ses-$SESSION/dwi/bvecs
        if [[ ! -f $bvecs ]];then
            echo "Gradient table not specified and convertable bvecs not found ($bvecs).  Unable to proceed."
            exit 1
        fi

        ${SCRIPTPATH}/lib/crush/create_gradient_patrix.py $bvecs $matrix

        if [[ ! -f $matrix ]];then
            echo "Gradient table ($matrix) could not be created from ($bvecs).  Unable to proceed."
            exit 1
        fi

    fi
    exit 0

}
export creategradientmatrix