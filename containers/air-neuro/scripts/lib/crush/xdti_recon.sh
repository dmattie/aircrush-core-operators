#!/bin/bash

############################################################
# dti_recon                                                #
############################################################
function f_dti_recon()
{
    #Params:
    #  1: path to 3D diffusion weighted image
    #  2: path to gradientmatrix file
    #  3: high b value (e.g. 1000)
    #  4: number of b0 rows in gradient matrix
  dwi=$1
  matrix=$2
  highb=$3
  b0=$4

  dti_recon $dwi "DTI_Reg2Brain" -gm $matrix -b $highb -b0 $b0 -p 3 -sn 1 -ot nii
  res=$?
  if [[ $res != 0 ]];then
    echo "FALSE"
  fi

  echo "TRUE"

}

export f_dti_recon
