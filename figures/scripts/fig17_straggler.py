import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import seaborn as sns

# DATA
avgjct_a = [544.43,	743.26]
avgjct_b = [573.0,	1610.0]

avgjct_b = [aj / avgjct_a[0] for aj in avgjct_b]
avgjct_a = [aj / avgjct_a[0] for aj in avgjct_a]
print(avgjct_a)
print(avgjct_b)
xs = ["w/o straggler", "w/ straggler"]

bar_width = 0.1

index = [0.45, 1.05]
index2 = [a+bar_width for a in index]

plt.rc('axes', axisbelow=True)
plt.grid(True, alpha=0.5, linewidth=0.75)

plt.ylabel("Normalized JCT")
plt.bar(index, avgjct_a, bar_width, edgecolor='black', linewidth=0.75, color='C1', label='FEDeS')
plt.bar(index2, avgjct_b, bar_width, edgecolor='black', linewidth=0.75, color='C2', label='VirtualFlow', hatch='x')
plt.xticks([a+bar_width/2 for a in index], xs, fontsize=10)
max_y = float(int(max(max(avgjct_a), max(avgjct_b)))) + 1.5
plt.yticks([float(i) for i in np.arange(0, 5)])
plt.ylim(0, 4)
plt.xlim(0.25, 1.25+bar_width)
plt.legend(ncol=1, loc='upper left', frameon=True, fontsize=10, framealpha=1)

plt.gcf().set_size_inches(2.8, 2) # w, h
plt.tight_layout()
plt.savefig(f"../images/fig17_straggler/straggler.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)
