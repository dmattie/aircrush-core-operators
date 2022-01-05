
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
    for eachnii in $SOURCE/dwi/sub-*.nii*;do
        dwifile=$eachnii
        break;
    done
    if [[ ! -f $dwifile ]];then
        echo "FALSE"
        >&2 echo "ERROR: Diffusion file not found matching search pattern : ($SOURCE/dwi/sub-*.nii*)"
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

  dti_recon $dwi "DTI_Reg2Brain" -gm $matrix -b $highb -b0 $b0 -p 3 -sn 1 -ot nii
  res=$?
  if [[ $res != 0 ]];then
    echo "FALSE"
  fi

  echo "TRUE"

}


############################################################
# odf_recon                                                #
############################################################
function f_odf_recon()
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

  nframes=`mri_info $dwi|grep nframes:|cut -d':' -f2|xargs`

  
  nframes=`mri_info $dwi|grep nframes:|cut -d':' -f2|xargs`
  if [[ ! $((nframes)) -gt 0 ]];then
    >&2 echo "ERROR: Unable to determine the number of frames in dwi file [$dwi]. Unable to proceed, number of directions must be known before performing odf_recon"
    return 1
  fi


  NUMBER_OF_DIRECTIONS=$((nframes+1))
  NUMBER_OF_OUTPUT_DIRS=181

  #odf_recon RAW_DATA_PREFIX NUMBER_OF_DIRECTIONS NUMBER_OF_OUTPUT_DIRS OUTPUT_FILE_PREFIX [OPTION]
  #~/bin/odf_recon data.nii 125 181 DTI_Recon -b0 1 -p 3 -sn 1 -ot nii -mat ~/projects/def-dmattie/HCP/100307/T1w/Diffusion/temp_mat.dat

  # cmdArray=["odf_recon",self.eddyCorrectedData,"31","181","%s/DTI_Recon" %(self.visit.tractographypath),"-b0", "5","-mat","%s/temp_mat.dat" %(self.visit.tractographypath),"-p","3","-sn", "1", "-ot", "nii"]

  odf_recon $dwi "ODF_Reg2Brain" $NUMBER_OF_DIRECTIONS $NUMBER_OF_OUTPUT_DIRS -mat $matrix -b0 $b0 -ot nii
  res=$?
  if [[ $res != 0 ]];then
    echo "FALSE"
  fi

  echo "TRUE"

}

############################################################
# recon                                                    #
############################################################
function f_diffusion_recon()
{
    dwifile=f_diffusion_exists


    if [[ $dwifile == "FALSE" ]];then
        >&2 echo "ERROR: Diffusion file not found matching search pattern : ($SOURCE/dwi/sub-*.nii*)."
        return 1
    fi
    #How many B values do we have.  If only one, we can use ODF recon, otherwise use DTI

    BVALS=$SOURCE/dwi/bvals
    if [[ ! -f $BVALS ]];then    
        >&2 echo "ERROR: $SOURCE/dwi/bvals not found.  Unable to continue, I need to know how many high b values I am working with"
        return 1
    fi

    #Find highest b val

    if [[ $BMAX == "" ]];then
        #Look at bvals file and find largest integer
        BMAX_VAL=`cat $SOURCE/dwi/bvals|tr ' ' '\n'|sort -u|grep -v '^0'|grep -v -e '^$'|sort -nr|head -1`
        BMAX="-b $BMAX_VAL"
        echo "Using high b value of $BMAX_VAL as per dwi/bvals file"
    else
        #Use passed value 
        BMAX_VAL=$BMAX
        BMAX="-b $BMAX"
        
    fi   

    num_high_b_vals=`cat $SOURCE/dwi/bvals|tr ' ' '\n'|sort -u|grep -v '^0'|grep -v -e '^$'|wc -l`
    if [[ $num_high_b_vals == '1' ]];then
        # ODF Recon can be used   
        echo "Performing ODF Recononstruction"
        if [[ $( f_odf_recon $dwifile $gradientmatrix $BMAX_VAL $num_high_b_vals) != "TRUE" ]];then
            >&2 echo "ERROR: odf_recon failed. Previous messages may contain a clue. Unable to proceed."
            return 1
        fi
        
    else        
        #Must use DTI_RECON            
        echo "Performing DTI Recononstruction"
        if [[ $( f_dti_recon $dwifile $gradientmatrix $BMAX_VAL $num_high_b_vals) != "TRUE" ]];then
            >&2 echo "ERROR: dti_recon failed. Previous messages may contain a clue. Unable to proceed."
            return 1
        fi
        
  
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