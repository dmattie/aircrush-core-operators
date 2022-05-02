#!/bin/bash
#SBATCH --time=10:00:00
#SBATCH --account=def-jlevman
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=32000M

SUBJECT=$1
SESSION=$2
PIPELINE=$3


singularity run --app registration ~/projects/rrg-jlevman/shared/projects/containers/air-neuro.sif \
    --datasetdir ~/projects/rrg-jlevman/shared/projects/abide/datasets \
    --subject $SUBJECT \
    --session $SESSION \
    --pipeline $PIPELINE &

singularity run --app parcellate ~/projects/rrg-jlevman/shared/projects/containers/air-neuro.sif \
    --datasetdir ~/projects/rrg-jlevman/shared/projects/abide/datasets \
    --subject $SUBJECT \
    --session $SESSION \
    --pipeline $PIPELINE &

wait

singularity run --app crush ~/projects/rrg-jlevman/shared/projects/containers/air-neuro.sif \
    --datasetdir ~/projects/rrg-jlevman/shared/projects/abide/datasets \
    --subject $SUBJECT \
    --session $SESSION \
    --pipeline $PIPELINE \
    --ix --swap_szc
