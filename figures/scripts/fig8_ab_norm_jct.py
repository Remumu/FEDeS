import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import seaborn as sns

# DATA
avgjct_a = []
avgjct_b = []

file_names = ["alpha-0", "alpha-1", "alpha-10", "srsf", "tiresias", "themis"]

for workload in ['a', 'b']:
    file_list = [
        f"../../logs/{workload}/{workload}-alpha-0.log", \
        f"../../logs/{workload}/{workload}-alpha-1.log", \
        f"../../logs/{workload}/{workload}-alpha-10.log", \
        f"../../logs/{workload}/{workload}-a-srsf.log", \
        f"../../logs/{workload}/{workload}-a-tiresias.log", \
        f"../../logs/{workload}/{workload}-a-themis.log"
    ]

    if workload == 'a':
        num_job = 100
    else:
        num_job = 50

    for i, f in enumerate(file_list):
        jcts = []
        try:
            with open(f, "r") as file:
                line = file.readline()
                while True:
                    line = file.readline()
                    if i < 3:
                        if "user" not in line:
                            break
                        segs = line.rstrip().split(',')
                        jind = int(segs[0][4:]) - 1
                        jct = float(segs[6])
                    else:
                        if "Job" not in line:
                            break
                        segs = line.rstrip().split(',')
                        jind = int(segs[0][3:]) - 1
                        jct = float(segs[3]) / 1000.0
                    
                    jcts.append(jct)
        except:
            pass
        try:
            avgjct = sum(jcts) / len(jcts)
        except:
            avgjct = 1.0
        if workload == 'a':
            avgjct_a.append(avgjct)
        else:
            avgjct_b.append(avgjct)

print(avgjct_a)
print(avgjct_b)
avgjct_a = [aj / avgjct_a[0] for aj in avgjct_a]
avgjct_b = [aj / avgjct_b[0] for aj in avgjct_b]
print(avgjct_a)
print(avgjct_b)
xs = ["α=0", "α=1", "α=∞", "SRSF", "Tiresias", "Themis"]

bar_width = 0.3

index = [0.5, 2, 3.5, 5, 6.5, 8]
index2 = [a+bar_width for a in index]

plt.rc('axes', axisbelow=True)
plt.grid(True, alpha=0.5, linewidth=0.75)

plt.ylabel("Normalized AVG JCT")
plt.bar(index, avgjct_a, bar_width, edgecolor='black', linewidth=0.75, color='C1', label='Trace A')
plt.bar(index2, avgjct_b, bar_width, edgecolor='black', linewidth=0.75, color='C2', label='Trace B', hatch='x')
plt.xticks([a+bar_width/2 for a in index], xs, fontsize=9, rotation=20)
max_y = float(int(max(max(avgjct_a), max(avgjct_b)))) + 0.5
plt.yticks([i for i in np.arange(0, max_y + 0.5, 0.5)])
plt.ylim(0, max_y)
plt.legend(ncol=2, loc='upper left', frameon=True, fontsize=7, framealpha=1)

plt.gcf().set_size_inches(3.5, 2) # w, h
plt.tight_layout()
plt.savefig(f"../images/fig8_ab_norm_jct/ab_norm_jct.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)