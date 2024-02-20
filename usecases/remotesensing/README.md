# Instrucctions to run Remote Sensing App with Hercules on Torino cluster using Spack.
# Install Hercules with Spack.
spack install hercules

# Edit Hercules' configuration file.
# Change the 

# Run the slurm script.
sbatch -N3 hercules_torino-1server-1process-in-1node-attached.conf

 
