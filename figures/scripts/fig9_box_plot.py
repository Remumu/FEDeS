import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

file_names = ["alpha-0", "alpha-1", "alpha-10", "a-srsf", "a-tiresias", "a-themis"]
xs = ["α = 0", "α = 1", "α = ∞", "SRSF", "Tiresias", "Themis"]
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

    fairs = []
    max_fair = 0
    for i, f in enumerate(file_list):
        fair = []
        fair_sorted = []
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
                        f = float(segs[11])
                    else:
                        if "Job" not in line:
                            break
                        segs = line.rstrip().split(',')
                        jind = int(segs[0][3:]) - 1
                        f = float(segs[6])
                    
                    fair.append(f)

            print(f"{workload}, {file_names[i]}, min, {min(fair)}, max, {max(fair)}, max/min, {max(fair)/min(fair):.3f}")
            fair_sorted = np.sort(fair)
            max_fair = max(max_fair, max(fair))
        except:
            pass
        fairs.append(fair_sorted)

    linewidth = 1.2
    p = 1. * np.arange(num_job) / (num_job - 1)
    plt.rc('axes', axisbelow=True)
    plt.grid(True, alpha=0.5, linewidth=0.75)   
    plt.ylabel('Fairness')
    for i, f in enumerate(file_names):
        plt.grid(True, axis='y')
        bp = plt.boxplot(fairs, whis=(0,100), showfliers=True)
        result_list = []
        for t in range(len(fairs)):
            result_dict = {}
            result_dict['label'] = f
            result_dict['min'] = min(fairs[t])
            result_dict['lower_whisker'] = bp['whiskers'][t*2].get_ydata()[1]
            result_dict['lower_quartile'] = bp['boxes'][t].get_ydata()[1]
            result_dict['median'] = bp['medians'][t].get_ydata()[1]
            result_dict['upper_quartile'] = bp['boxes'][t].get_ydata()[2]
            result_dict['upper_whisker'] = bp['whiskers'][(t*2)+1].get_ydata()[1]
            result_dict['max'] = max(fairs[t])
            result_dict['max-min'] = max(fairs[t]) - min(fairs[t])
            result_dict['IQR'] = result_dict['upper_quartile'] - result_dict['lower_quartile']
            result_list.append(result_dict)
    print(pd.DataFrame(result_list))
    plt.xticks([k + 1 for k in range(len(file_names))], xs, fontsize=9, rotation=20)
    plt.ylim(0, 4)
    plt.gcf().set_size_inches(3.5, 2.0)
    plt.tight_layout()
    plt.savefig(f'../images/fig9_box_plot/box-fair-{workload}.pdf', dpi=300, bbox_inches='tight', pad_inches = 0.02)
    plt.clf()