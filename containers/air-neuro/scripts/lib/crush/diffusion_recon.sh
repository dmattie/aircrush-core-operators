
#!/bin/bash

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
#source "${SCRIPTPATH}/lib/crush/dti_recon.sh"
#source "${SCRIPTPATH}/lib/crush/odf_recon.sh"


############################################################
# diffusion exists
############################################################

function f_diffusion_exists()
{
    shopt -s globstar  
    # for eachnii in $SOURCE/dwi/sub-*.nii*;do
    #     dwifile=$eachnii
    #     break;
    # done
    dwifile=$TARGET/reg2brain.data.nii.gz
    #     dwifile=$eachnii
    #     break;
    # done
    if [[ ! -f $dwifile ]];then
        echo "FALSE"
        >&2 echo "ERROR: Diffusion file not found matching search pattern : ($TARGET/reg2brain.data.nii.gz)"
        exit 1
    fi
    echo $dwifile
}


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
  shift;shift;shift;shift;
  echo "f_dti_recon extras:{$@}"

  cd $TARGET

  if [[ -f $TARGET/dti_recon_out_fa 
     && -f $TARGET/dti_recon_out_adc.nii
     && -f $TARGET/dti_recon_out_dwi.nii ]];then
    echo "Previous dti_recon_out output appears to exist. Skipping dti_recon"
    return 2
  fi

  dti_recon $dwi "dti_recon_out" -gm $matrix -b $highb -b0 $b0 -p 3 -sn 1 -ot nii
  res=$?   
  if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to complete dti_recon"
    return 1
  fi
  if [[ ! -f "crush_dti.trk" ]];then
    dti_tracker "dti_recon_out" "crush_dti.trk" -m dti_recon_out_dwi.nii -it "nii" "$@"      
  fi
  return $?

}


############################################################
# odf_recon                                                #
############################################################
function f_odf_recon()
{
    #Params:
    #  1: path to 3D diffusion weighted image
    #  2: high b value (e.g. 1000)
    #  3: number of b0 rows in gradient matrix
  dwi=$1
  highb=$2
  b0=$3
  shift;shift;shift;
  echo "f_odf_recon extras:{$@}"

  
  cd $TARGET

  if [[ -f $TARGET/recon_out_odf.nii && -f $TARGET/recon_out_max.nii && -f $TARGET/recon_out_b0.nii && -f $TARGET/recon_out_dwi.nii ]];then
    echo "Previous odf_recon output detected. Skipping odf_recon"
    if [[ ! -f $TARGET/crush.trk ]];then
        echo "No track file detected.  Tracking $TARGET/crush_qball.trk"
        echo odf_tracker "recon_out" "crush_qball.trk" -m recon_out_dwi.nii -it "nii" "$@"        
        odf_tracker "recon_out" "crush_qball.trk" -m recon_out_dwi.nii -at 35 -it "nii" "$@"        
        return $?
    fi    
    return 2
  fi

  nframes=`mri_info $dwi|grep nframes:|cut -d':' -f2|xargs`
  if [[ ! $((nframes)) -gt 0 ]];then
    >&2 echo "ERROR: Unable to determine the number of frames in dwi file [$dwi]. Unable to proceed, number of directions must be known before performing odf_recon"    
    return 1
  fi

  measurementpoints=`cat $TARGET/gradientmatrix_qball.txt|wc -l`

  #NUMBER_OF_DIRECTIONS=$((nframes+0))
  NUMBER_OF_DIRECTIONS=$((measurementpoints+0))
  NUMBER_OF_OUTPUT_DIRS=181  
  echo odf_recon $dwi $NUMBER_OF_DIRECTIONS $NUMBER_OF_OUTPUT_DIRS "recon_out" -mat $TARGET/hardi_mat_qball.dat -b0 $b0 -ot nii -p 3 -sn 1
  odf_recon $dwi $NUMBER_OF_DIRECTIONS $NUMBER_OF_OUTPUT_DIRS "recon_out" -mat $TARGET/hardi_mat_qball.dat -b0 $b0 -ot nii -p 3 -sn 1

  res=$?
  if [[ $res != 0 ]];then
    >&2 echo "ERROR: Unable to complete odf_recon"    
    return 1
  fi
  if [[ ! -f "crush_qball.trk" ]];then
    echo odf_tracker "recon_out" "crush_qball.trk" -m recon_out_dwi.nii -it "nii" "$@"
    odf_tracker "recon_out" "crush_qball.trk" -m recon_out_dwi.nii -at 35 -it "nii" "$@"  
  fi
  return $?


}

############################################################
# recon                                                    #
############################################################
function f_diffusion_recon()
{    
    dwifile=$( f_diffusion_exists )
   # gradientmatrix=$TARGET/gradientmatrix.txt


    if [[ $dwifile == "FALSE" ]];then
        >&2 echo "ERROR: Diffusion file not found matching search pattern : ($TARGET/reg2brain.data.nii.gz)."        
        return 1
    fi
    #How many B values do we have.  If only one, we can use ODF recon, otherwise use DTI
    #Todo support for a "bvecs" filename is non compliant and needs to be deprecated.
    BVALS=$SOURCE/dwi/bvals

    if [[ ! -f $BVALS ]];then
        #Lets find a bids compliant bvals filename supporting multiple runs (we'll take the first one we find)
        shopt -s globstar
        for eachbval in $SOURCE/dwi/sub-${SUBJECT}_ses-${SESSION}_*run-*_dwi.bval; do
            BVALS=$eachbval
            break;
        done
    fi

    if [[ ! -f $BVALS ]];then    
        >&2 echo "ERROR: $SOURCE/dwi/bvals not found.  Unable to continue, I need to know how many high b values I am working with"        
        return 1
    fi

    #Find highest b val

    if [[ $BMAX == "" ]];then
        #Look at bvals file and find largest integer
        BMAX_VAL=`cat $BVALS|tr ' ' '\n'|sort -u|grep -v '^0'|grep -v -e '^$'|sort -nr|head -1`
        BMAX="-b $BMAX_VAL"
        echo "Using high b value of $BMAX_VAL as per dwi/bvals file"
    else
        #Use passed value 
        BMAX_VAL=$BMAX
        BMAX="-b $BMAX"
        
    fi   

    num_high_b_vals=`cat $BVALS|tr ' ' '\n'|sort -u|grep -v '^0'|grep -v -e '^$'|wc -l`
    if [[ $num_high_b_vals == '1' ]];then
        # ODF Recon can be used     
        echo "Performing ODF Reconstruction"      
        res=$( f_odf_recon $dwifile $BMAX_VAL $num_high_b_vals "$@")        
        res_code=$?
        if [[ $res_code != 0 ]];then
            echo $res
            if [[ $res_code == 2 ]];then
                return 0
            fi        
            >&2 echo "ERROR: odf_recon failed. Previous messages may contain a clue. Unable to proceed."            
            return 1
        fi
    fi             
    #Lets also use DTI_RECON, we need the FA maps anyway
    echo "Performing DTI Recononstruction"

    res=$( f_dti_recon $dwifile $TARGET/gradientmatrix_dti.txt $BMAX_VAL $num_high_b_vals "$@" ) 
    res_code=$?
    if [[ $res_code != 0 ]];then        
        if [[ $res_code == 2 ]];then            
            return 0
        fi        
        >&2 echo "ERROR: dti_recon failed. Previous messages may contain a clue. Unable to proceed."
        return 1
    fi
               
    return 0

}

export f_diffusion_recon



# dwifiles=os.listdir(args.diffusionpath)
#     for f in dwifiles:
#         if f.endswith('bvec'):
#             bvec_fname=f"{args.diffusionpath}/{f}"
#             break  #Get the first one I can find, we are only processing the first scan of this session
#         if f=='bvecs':
#             bvec_fname=f"{args.diffusionpath}/{f}"
#             break

#  dti_recon $TARGET/reg2brain.data.nii.gz $TARGET/DTI_Reg2Brain -gm $TARGET.gradientmatrix.txt $BMAX $BNOT -p 3 -sn 1 -ot nii


# python $CRUSH_PATH/crush.py -samples $SUBJECTS_DIR -patient sub-$patientID -recrush -fixmissing #-gradienttable ~/projects/def-dmattie/crush/plugins/levman/hcp_gradient_table_from_data_dictionary_3T.csv
# pwd

# if [ -f "$SUBJECTS_DIR/sub-$patientID/ses-$sessionID/Tractography/crush/tracts.txt" ]; then