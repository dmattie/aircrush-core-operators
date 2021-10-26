#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow
label: "Levman"
inputs:
    subject: string
    session: string    
        
outputs:
    wmparc:
        type: File
        outputsource: recon/wmparc
steps:
    recon:
        run: singularity.cwl
        in:
            subject:subject
            session:session
        out:
            [wmparc]

