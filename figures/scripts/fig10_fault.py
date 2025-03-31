import matplotlib.pyplot as plt

FEDeS_no_faults = []
FEDeS_10_faults = []
FEDeS_20_faults = []
FEDeS_50_faults = []

ARF_no_faults = []
ARF_10_faults = []
ARF_20_faults = []
ARF_50_faults = []

FEDeS_fault_filename = '../../logs/fault/FEDeS_resnet_fault_log'
ARF_fault_filename = '../../logs/fault/ARF_resnet_fault_log'

plt.grid(True, alpha=0.5, linewidth=0.75)
plt.xlabel('Time (h)', fontsize=10)
plt.ylabel('Validation\nAccuracy (%)', fontsize=10)
with open(FEDeS_fault_filename) as f:
    for l in f.readlines():
        no_fault_time, no_fault_acc, fault_10_time, fault_10_acc, fault_20_time, fault_20_acc, fault_50_time, fault_50_acc = l.split('\t')
        no_fault_time = float(no_fault_time) / 3600
        no_fault_acc = float(no_fault_acc) * 100
        fault_10_time = float(fault_10_time) / 3600
        fault_10_acc = float(fault_10_acc) * 100
        fault_20_time = float(fault_20_time) / 3600
        fault_20_acc = float(fault_20_acc) * 100
        fault_50_time = float(fault_50_time) / 3600
        fault_50_acc = float(fault_50_acc) * 100

        FEDeS_no_faults.append((no_fault_time, no_fault_acc))
        FEDeS_10_faults.append((fault_10_time, fault_10_acc))
        FEDeS_20_faults.append((fault_20_time, fault_20_acc))
        FEDeS_50_faults.append((fault_50_time, fault_50_acc))

with open(ARF_fault_filename) as f:
    for l in f.readlines():
        no_fault_time, no_fault_acc, fault_10_time, fault_10_acc, fault_20_time, fault_20_acc, fault_50_time, fault_50_acc = l.split('\t')
        no_fault_time = float(no_fault_time) / 3600
        no_fault_acc = float(no_fault_acc) * 100
        fault_10_time = float(fault_10_time) / 3600
        fault_10_acc = float(fault_10_acc) * 100
        fault_20_time = float(fault_20_time) / 3600
        fault_20_acc = float(fault_20_acc) * 100
        fault_50_time = float(fault_50_time) / 3600
        fault_50_acc = float(fault_50_acc) * 100

        ARF_no_faults.append((no_fault_time, no_fault_acc))
        ARF_10_faults.append((fault_10_time, fault_10_acc))
        ARF_20_faults.append((fault_20_time, fault_20_acc))
        ARF_50_faults.append((fault_50_time, fault_50_acc))

plt.plot([t for t, a in FEDeS_50_faults], [a for t, a in FEDeS_50_faults], label='FEDeS 50 faults', zorder=5, color='C3', linestyle='solid', markevery=[-1], marker='o', markersize=3)
plt.axvline(FEDeS_50_faults[-1][0], ymin=0, ymax=80, color='C3', linestyle='solid')

plt.plot([t for t, a in ARF_no_faults], [a for t, a in ARF_no_faults], label='ARF no faults', zorder=4, color='C2', linestyle='dashed', markevery=[-1], marker='D', markersize=3)
plt.axvline(ARF_no_faults[-1][0], ymin=0, ymax=80, color='C2', linestyle='dashed')

plt.plot([t for t, a in ARF_20_faults], [a for t, a in ARF_20_faults], label='ARF 20 faults', zorder=2, color='C0', linestyle=(0, (3, 1)), markevery=[-1], marker='^', markersize=3)
plt.axvline(ARF_20_faults[-1][0], ymin=0, ymax=80, color='C0', linestyle=(0, (3, 1)))

plt.plot([t for t, a in ARF_50_faults], [a for t, a in ARF_50_faults], label='ARF 50 faults', zorder=1, color='C4', linestyle=(0, (5, 1)), markevery=[-1], marker='p', markersize=3)
plt.axvline(ARF_50_faults[-1][0], ymin=0, ymax=80, color='C4', linestyle=(0, (5, 1)))

plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0, 1.1, 1, 0.2))
plt.ylim(0, 80)
plt.xlim(0, 30)

plt.tight_layout(rect=[0, 0.1, 1, 1])

plt.gcf().set_size_inches(7, 2.5)

plt.savefig('../images/fig10_fault/fault.pdf')
