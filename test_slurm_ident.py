from util.sensors import is_slurm_script

res=is_slurm_script('/tmp/my_slurm.sl')
print(res)