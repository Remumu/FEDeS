# FEDeS simulator
This is a simulator for Large Scale Experiments in FEDeS.
We measured the iteration time and initialization time according to the number of GPUs used and their node locality for each model and predicted the results of 137 days of trace experiments on a cluster consisting of up to 144 GPUs.
The difference from the cluster experiment run at a scale of 40 GPUs and about 10 hours was 3.01%.

## Quickstart
Build simulator
``` bash
./compile.sh
```
Run scale-down trace
``` bash
./run_sim_arf.sh
./run_sim_alpha.sh
```
Run large-scale trace
``` bash
./run_sim_ls_arf.sh
./run_sim_ls_alpha.sh
```

In the case of `run_sim_ls_alpha.sh`, it takes about an hour. When simulating FEDeS, the number of loops in the simulator increases because the job is split into functions and simulated as execution units.
The overhead of dynamic programming implemented to calculate the GPU allocation of alpha scheduling was up to 23ms.