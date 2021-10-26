$namespaces:
  sbg: 'https://www.sevenbridges.com/'
id: singularity
label: Singularity
class: CommandLineTool
cwlVersion: v1.0
inputs: 
    container:
        type: File
        inputBinding:
            position: 2
    operator:
        type: string
        inputBinding:
            1
        
        
outputs: 
    freesurfer:
        type: File
        path: Freesurfer/mri/wmparc.mgz
        
baseCommand: [singularity, run, --app']
doc: ''
