
#!/bin/bash

############################################################
# diffusion exists
############################################################

diffusion_exists()
{
    shopt -s globstar  
    for eachnii in $SOURCE/dwi/sub-*.nii*;do
        dwifile=$eachnii
        break;
    done
    if [[ ! -f $dwifile ]];then
        >&2 echo "ERROR: Diffusion file not found matching search pattern : ($SOURCE/dwi/sub-*.nii*)"
        exit 1
    fi
    echo "TRUE"
}
############################################################
# recon                                                    #
############################################################
recon()
{

    if [[ $( diffusion_exists ) != "TRUE" ]];then
        >&2 echo "ERROR: Diffusion file not found matching search pattern : ($SOURCE/dwi/sub-*.nii*)."
        exit 1
    fi
    #How many B values do we have.  If only one, we can use ODF recon, otherwise use DTI

    BVALS=$SOURCE/dwi/bvals
    if [[ ! -f $BVALS ]];then    
        >&2 echo "ERROR: $SOURCE/dwi/bvals not found.  Unable to continue, I need to know how many high b values I am working with"
        exit 1
    fi

    #Find highest b val

    if [[ $BMAX == "" ]];then
        #Look at bvals file and find largest integer
        BMAX_VAL=`cat $SOURCE/dwi/bvals|tr ' ' '\n'|sort -u|grep -v '^0'|grep -v -e '^$'|sort -nr|head -1`
        BMAX="-b $BMAX_VAL"
        echo "Using high b value of $BMAX_VAL as per dwi/bvals file"
    else
        #Use passed value 
        BMAX="-b $BMAX"
    fi   

    num_high_b_vals=`cat $SOURCE/dwi/bvals|tr ' ' '\n'|sort -u|grep -v '^0'|grep -v -e '^$'|wc -l`
    if [[ $num_high_b_vals == '1' ]];then
        # ODF Recon can be used   
        echo "Performing ODF Recononstruction"
        
    else
        echo "Performing DTI Reconstruction"
    #Must use DTI_RECON
        # dti_recon ",self.eddyCorrectedData,"%s/DTI_Reg2Brain" %(self.visit.tractographypath),"-gm",defaultGradientMatrix,"-b", "1000","-b0",self.visit.b0,"-p","3","-sn","1","-ot","nii"]


    fi

}

export recon


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