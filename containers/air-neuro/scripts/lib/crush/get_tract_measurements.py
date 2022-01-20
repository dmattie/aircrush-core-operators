#!/usr/bin/env python3
 
import sys,os,subprocess
import re
import nibabel as nib
import numpy as np
import warnings
import json
import argparse

def process(**kwargs):#segment,counterpart,method):
        segment=kwargs['roi_start']
        counterpart=kwargs['roi_end']        
        method=kwargs['method']
        tractographypath=os.path.dirname(kwargs['tract_file'])
        tract=kwargs['tract_file']
        pipelineId=kwargs['pipeline']
        calcs={}
        #track_vis ./DTI35_postReg_Threshold5.trk -roi_end ./wmparc3001.nii.gz -roi_end2 ./wmparc3002.nii.gz -nr
        

        wmparcStart=f"{tractographypath}/parcellations/wmparc{segment}.nii"
        wmparcEnd=f"{tractographypath}/parcellations/wmparc{counterpart}.nii"
        

        if os.path.isfile(wmparcStart) and os.path.isfile(wmparcEnd):

            trackvis = ["track_vis",tract,f"-{method}",wmparcStart,f"-{method}2",wmparcEnd,"-nr", "-ov",f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii","-disable_log"]
            
            if not os.path.isdir(f"{tractographypath}/crush/"):
                os.mkdir(f"{tractographypath}/crush/")



            if not os.path.isfile(f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii"):
                if not os.path.isdir(f"{tractographypath}/crush/{segment}"):
                    os.mkdir(f"{tractographypath}/crush/{segment}")
                try:

                    with open(f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii.txt", "w") as track_vis_out:
                        proc = subprocess.Popen(trackvis, stdout=track_vis_out)
                        proc.communicate() 
                        
                except Exception as e:
                    print(f"Trackvis failed::{e}")                            

            with open (f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii.txt" %(tractographypath,segment,segment,counterpart,method), "r") as myfile:                        
                data=myfile.read()                                   

            
            
            m = re.search(r'Number of tracks: (\d+)', data)
            if m:
                NumTracts = m.group(1).strip()
            else:
                NumTracts = 0
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-NumTracts"]=NumTracts
            
            ############
            
            m = re.search(r'Number of tracks to render: (\d+)', data)
            if m:
                TractsToRender = m.group(1).strip()
            else:
                TractsToRender = 0
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-TractsToRender"]=TractsToRender
            
            ############
            
            m = re.search(r'Number of line segments to render: (\d+)', data)
            if m:
                LinesToRender = m.group(1).strip()
            else:
                LinesToRender = 0
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-LinesToRender"]=LinesToRender
            ############
            
            m = re.search(r'Mean track length: (\d+.\d+) \+\/- (\d+.\d+)', data)
            if m:
                MeanTractLen = m.group(1).strip()
                MeanTractLen_StdDev = m.group(2).strip()
            else:
                MeanTractLen = 0
                MeanTractLen_StdDev = 0
            
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-MeanTractLen" ]=MeanTractLen
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-MeanTractLen_StdDev"]=MeanTractLen_StdDev
            ############
            
            m = re.search(r'Voxel size: (\d*[.,]?\d*) (\d*[.,]?\d*) (\d*[.,]?\d*)', data)
            if m:
                VoxelSizeX = m.group(1).strip()
                VoxelSizeY = m.group(2).strip()
                VoxelSizeZ = m.group(3).strip()
            else:
                VoxelSizeX = 0
                VoxelSizeY = 0
                VoxelSizeZ = 0

            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-VoxelSizeX"]=VoxelSizeX
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-VoxelSizeY"]=VoxelSizeY
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-VoxelSizeZ"]=VoxelSizeZ

            
            #FA Mean
            meanFA=nonZeroMean(f"{tractographypath}/dti_recon_out_fa.nii" ,f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii")                             
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-meanFA"]=meanFA
            
            #FA Std Dev
            stddevFA=nonZeroStdDev(f"{tractographypath}/dti_recon_out_fa.nii",f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii" )         
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-stddevFA"]=stddevFA            
            
            #ADC Mean
            meanADC=nonZeroMean(f"{tractographypath}/dti_recon_out_adc.nii",f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii" )         
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-meanADC"]=meanADC
            
            #ADC Std Dev
            stddevADC=nonZeroStdDev(f"{tractographypath}/dti_recon_out_adc.nii",f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii" )       
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-stddevADC"]=stddevADC
            
            #Volume                
            volume=volume_in_voxels(f"{tractographypath}/dti_recon_out_adc.nii",f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii" )                                                      
            calcs[f"{pipelineId}/{segment}-{counterpart}-{method}-voxelvolume"]=volume
            
            

        else:
            #print("Parcellation (wmparc####.nii) files missing (%s or %s)"%(segment,counterpart))
            if not os.path.isfile(wmparcStart):
                print(f"{wmparcStart} is missing for {segment}-{counterpart} operation")
            if not os.path.isfile(wmparcEnd):
                print(f"{wmparcStart} is missing for {segment}-{counterpart} operation" %(wmparcStart,segment,counterpart))
        

        try:

            if not os.path.isdir(f"{tractographypath}/crush/{segment}" ):
                os.mkdir(f"{tractographypath}/crush/{segment}" )

            calcsJson = f"{tractographypath}/crush/{segment}/calcs-{segment}-{counterpart}-{method}.json" 
            
            with open(calcsJson, "w") as calcs_file:
                json.dump(calcs,calcs_file)

        except Exception as e:
            raise Exception(f"dump failed::{e}")  

        nii = f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii" 
        datafile = f"{tractographypath}/crush/{segment}/{segment}-{counterpart}-{method}.nii.txt" 
                
        if os.path.isfile(nii):
            os.unlink(nii) 
        
        if os.path.isfile(datafile):
            os.unlink(datafile)
                
        
        return json.dumps(calcs)   # dict doesn't appear to be threadsafe, need to stringify

def volume_in_voxels(adcFile,roiFile):
    
    if os.path.isfile(adcFile) == False:        
        print(f"{adcFile} is MISSING")
        return
    if os.path.isfile(roiFile) == False:        
        print(f"{roiFile} is MISSING" )
        return

    imgADC = nib.load(adcFile) #Untouched
    dataADC = imgADC.get_data()
    
    img = nib.load(roiFile)
    roiData = img.get_data()

    indecesOfInterest = np.nonzero(roiData)        

    #I expect to see runtime warnings in this block, e.g. divide by zero
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)                 
        volume =np.count_nonzero(dataADC[indecesOfInterest])            
    return volume

def nonZeroMean(faFile,roiFile):
    
    if os.path.isfile(faFile) == False:        
        print(f"{faFile} is MISSING" )
        return
    if os.path.isfile(roiFile) == False:        
        print(f"{roiFile} is MISSING" )
        return

    imgFA = nib.load(faFile) #Untouched
    dataFA = imgFA.get_data()
    
    img = nib.load(roiFile)
    roiData = img.get_data()

    indecesOfInterest = np.nonzero(roiData)
    

    #I expect to see runtime warnings in this block, e.g. divide by zero
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)                 
        mean =np.mean(dataFA[indecesOfInterest],dtype=np.float64)
        #print(mean)        
    return mean
def nonZeroStdDev(faFile,roiFile):
    
    if os.path.isfile(faFile) == False:        
        print(f"{faFile} is MISSING" )
        return
    if os.path.isfile(roiFile) == False:        
        print(f"{roiFile} is MISSING" )
        return

    imgFA = nib.load(faFile) #Untouched
    dataFA = imgFA.get_data()

    img = nib.load(roiFile)
    roiData = img.get_data()

    indecesOfInterest = np.nonzero(roiData)

    #I expect to see runtime warnings in this block, e.g. Degrees of freedom <= 0 for slice
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)            
        std =np.std(dataFA[indecesOfInterest],dtype=np.float64)

    return std

def main():

    args=None

    parser = argparse.ArgumentParser(
        description="CRUSH client command line utility. Extract measurements from two ROI files and a tract file.")
    parser.add_argument('-roi_start',action='store', help="ROI file (.nii) at the start of the tract")
    parser.add_argument('-roi_end',action='store', help="ROI file (.nii) at the end of the tract")
    parser.add_argument('-method',action='store', help="{roi|roi_end} Look at any tracts that touch the specified ROIs (roi) or just those tracts that terminate in the ROI (roi_end)")
    parser.add_argument('-tract',action='store', help="Path to the tract file")
    parser.add_argument('-pipeline',action='store', help="The name of the pipeline being processed to tag the data as it is stored")    
    args = parser.parse_args()

    process(roi_start=args.roi_start,roi_end=args.roi_end,method=args.method,tract_file=args.tract,pipeline=args.pipeline)

if __name__ == '__main__':
    main()
