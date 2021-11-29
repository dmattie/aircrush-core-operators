
#!/bin/bash

############################################################
# hardi_mat                                                     #
############################################################
hardi_mat()
{
    matrix=$1

    if [[ -f $matrix ]];then
        echo "Matrix file already exists. I will use it."
        exit 0
    fi

    hardi_mat $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION/gradientmatrix.txt \
        $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION/hardi_mat.dat \
        -ref $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION/reg2brain.data.nii.gz \
        -oc
    $ret=$?
    if [[ $ret -ne 0 ]];then
        >&2 echo "ERROR: hardi_mat failed with error. see above."
        exit $ret
    fi

}
export hardi_mat
