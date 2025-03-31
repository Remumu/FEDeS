import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import seaborn as sns

base_dir = '../../logs'

trace_names = [
    'ee9e8c', '2869ce', '11cb48', '6c71a0', 'b436b2', 
    '103959', '0e4a51', '7f04ca', 'e13805'
]

schedulers = ["α = 0", "α = 1", "α = ∞", "SRSF", "Tiresias", "Themis"]

vc_jcts = []
vc_fairnesses = []
for trace_name in trace_names:
    vc_jct = []
    vc_fairness = []
    cluster_size = 1
    with open(f"../../traces/philly-traces/analysis/vldb_traces/{trace_name}/vs7.topology") as file:
        for i in range(3):
            line = file.readline().rstrip()
            cluster_size *= int(line)
    print(f"{trace_name}, makespan, avgjct, fairness, avg_gpu_load, avg_gpu_load/{cluster_size}")
    for alpha in [0, 1, 10]:
        try:
            with open(f"{base_dir}/{trace_name}/s-alpha-zero-{alpha}/{trace_name}-summary.stats") as file:
                while True:
                    line = file.readline()
                    if not line:
                        break
                    if "Makespan" in line:
                        segs = line.rstrip().split(",")
                        print(f"alpha-{alpha}, {int(segs[1])}, {float(segs[3]):.2f}, {float(segs[5]):.3f}, {float(segs[7]):.2f}, {float(segs[7])/cluster_size:.2f}")
                        vc_jct.append(float(segs[3]))
                        vc_fairness.append(float(segs[5]))
        except:
            print(f"alpha-{alpha}, ")
            vc_jct.append(float(1))
            vc_fairness.append(float(1))

    for scheduler in ['srsf', 'las', 'themis']:
        try:
            with open(f"{base_dir}/{trace_name}/{scheduler}/{trace_name}-summary.stats") as file:
                while True:
                    line = file.readline()
                    if not line:
                        break
                    if "Makespan" in line:
                        segs = line.rstrip().split(",")
                        print(f"{scheduler}, {int(segs[1])}, {float(segs[3]):.2f}, {float(segs[5]):.3f}, {float(segs[7]):.2f}, {float(segs[7])/cluster_size:.2f}")
                        vc_jct.append(float(segs[3]))
                        vc_fairness.append(float(segs[5]))
        except:
            print(f"{scheduler}, ")
            vc_jct.append(float(1))
            vc_fairness.append(float(1))
    vc_jcts.append(vc_jct)
    vc_fairnesses.append(vc_fairness)

index_default = [0.5, 0.7, 0.9, 1.1, 1.3, 1.5]
bar_width = 0.2
for i in range(len(vc_jcts)):
    vc_jct = vc_jcts[i]
    vc_jct = [jct / vc_jct[0] for jct in vc_jct]

    index = [ind + 2.0 * i for ind in index_default]
    plt.rc('axes', axisbelow=True)
    plt.grid(True, alpha=0.5, linewidth=0.75)

    plt.ylabel("Normalized AVG JCT")
    for j in range(len(index)):
        if i == 0:
            plt.bar(index[j], vc_jct[j], bar_width, edgecolor='black', linewidth=0.5, color=f'C{j}', label=f"{schedulers[j]}")
        else:
            plt.bar(index[j], vc_jct[j], bar_width, edgecolor='black', linewidth=0.5, color=f'C{j}')
xs = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
plt.legend(ncol=6, loc='upper center', frameon=True, fontsize=9, framealpha=1)
plt.xticks([1 + i * 2.0 for i in range(9)], xs, fontsize=9)
plt.ylim(0, 5)
plt.gcf().set_size_inches(7.2, 2) # w, h
plt.tight_layout()
plt.savefig(f"../images/fig16_ls_norm_jct/ls_norm_jcts.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)