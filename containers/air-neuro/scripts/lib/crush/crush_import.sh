#!/bin/bash
source "${SCRIPTPATH}/lib/crush/hardi_mat.sh"
source "${SCRIPTPATH}/lib/crush/creategradientmatrix.sh"
source "${SCRIPTPATH}/lib/crush/dti_recon.sh"
source "${SCRIPTPATH}/lib/crush/diffusion_recon.sh"

export hardi_mat
export creategradientmatrix
export dti_recon
export diffusion_recon