# FEDeS

This repository contains the code and scripts for generating figures of FEDeS.

## Structure

```
.
├── figures/         # Contains all figure generation scripts
│   ├── scripts/     # Python scripts for generating each figure
│   └── images/      # Generated figures are saved here
├── logs/            # Contains experiment and simulation logs
├── simulator/       # Contains source codes and scripts of simulator
└── traces/          # Contains workload traces
```

## Setup

1. Create a Python virtual environment (recommended):
```bash
python -m venv fedes
source fedes/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Generating Figures

The figures can be generated using the scripts in `figures/scripts/`. Each script corresponds to a specific figure in the paper:

- `fig3_jct_fair_tradeoff.py`: Figure 3 - Trade-off between fairness and efficiency with 𝛼-fairness
- `fig7_throughput.py`: Figure 7 - Throughput of FEDeS and ARF
- `fig8_ab_norm_jct.py`: Figure 8 - Average JCT normalized to 𝛼=0
- `fig9_box_plot.py`: Figure 9 - Box plot of job fairness values with various schedulers
- `fig10_fault.py`: Figure 10 - Training time and Validation Accuracy of ResNet50 under failures
- `fig11_lw_throughput.py`: Figure 11 - Average training throughput per LW over various numbers of phases normalized to that with 1 phase
- `fig12_13_queuing.py`: Figures 12-13 - Queuing time of each job in Trace A-B
- `fig14_15_gpu_util.py`: Figures 14-15 - Rate of utilized GPUs for Trace A-B where shaded portions in (d)-(f) signify idle GPUs despite having waiting jobs in the queue
- `fig16_ls_norm_jct.py`: Figure 16 - Average JCT normalized to 𝛼=0 over large-scale traces
- `fig17_straggler.py`: Figure 17 - Normalized JCTs with and without a straggler

To generate a figure, simply run the corresponding Python script:
```bash
python figures/scripts/figX_*.py
```

