class: Workflow
cwlVersion: v1.0
id: levman2
label: levman2
$namespaces:
  sbg: 'https://www.sevenbridges.com/'
inputs:
  - id: container
    'sbg:fileTypes': sif
    type: File
    'sbg:x': -723
    'sbg:y': -131
outputs: []
steps:
  - id: recon
    in:
      - id: container
        source: container
    out: []
    run: ./recon.cwl
    label: recon
    'sbg:x': -557.3984375
    'sbg:y': -27.5
  - id: singularity
    in: []
    out: []
    run: ./recon.cwl
    label: Singularity
    'sbg:x': -295.3984375
    'sbg:y': -37.5
requirements: []
