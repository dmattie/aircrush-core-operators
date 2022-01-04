
#!/bin/bash

############################################################
# hardi_mat                                                     #
############################################################
f_hardi_mat()
{
    echo "Calculating reconstruction matrix from gradient table"
    matrix=$1

    if [[ ! -f $matrix ]];then
        echo "hardi_mat:gradient atrix file missing[$matrix]."     
        return 1   
    fi

    hardi_mat $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION/gradientmatrix.txt \
        $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION/hardi_mat.dat \
        -ref $DATASETDIR/derivatives/$PIPELINE/sub-$SUBJECT/ses-$SESSION/reg2brain.data.nii.gz \
        -oc
    ret=$?
    if [[ $ret -ne 0 ]];then
        >&2 echo "ERROR: hardi_mat failed with error. see above."
        exit $ret
    fi

}
export f_hardi_mat
