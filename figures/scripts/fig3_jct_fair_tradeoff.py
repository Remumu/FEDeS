import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys

trace_names = ['a']

for trace_name in trace_names:
    print(f"{trace_name.upper()}")
    xs = [x/100 for x in range(0, 525, 25)]
    folder_prefixes = [
        # f"s-alpha-zero-fixedgbs-pknob", f"coldzero-s-alpha-zero-fixedgbs-pknob"
        f"s-alpha-zero-fixedgbs-pknob"
    ]
    for folder_prefix in folder_prefixes:
        makespans = []
        avgjcts = []
        fairnesses = []
        cold_start_per_jobs = []
        avg_waves = []
        for alpha in range(0, 525, 25):
            folder_name = f"{folder_prefix}-{alpha/100:3.2f}"
            base_dir = f'../../logs/{trace_name}/jct_fair_tradeoff'
            file_list = [f'{base_dir}/{folder_name}/{trace_name}-summary.stats']    
            for i, f in enumerate(file_list):
                try:
                    with open(f, "r") as file:
                        while True:
                            line = file.readline()
                            if not line:
                                break
                            if "Makespan" in line:
                                segs = line.rstrip().split(',')
                                makespan = int(segs[1])
                                avgjct = float(segs[3])
                                fairness = float(segs[5])
                                fairMax = float(segs[23])
                                fairMin = float(segs[25])
                                dfair = float(segs[27])
                                dfairsq = float(segs[29])
                                cspj = float(segs[9])
                                avg_wave = float(segs[11])
                                gpu_load = float(segs[7])
                                fairnessN = float(segs[31])
                                nfairMax = float(segs[33])
                                nfairMin = float(segs[35])
                                dfairN = float(segs[37])
                                dfairsqN = float(segs[39])
                                makespans.append(makespan)
                                avgjcts.append(avgjct)
                                fairnesses.append(fairness)
                                cold_start_per_jobs.append(cspj)
                                avg_waves.append(avg_wave)
                                line = file.readline()
                except:
                    print(f'{f}')
                    pass
        fig, ax1 = plt.subplots()
        plt.grid(True)
        l1, = ax1.plot(xs, avgjcts, color = 'blue', label='AVG JCT', linewidth=1, marker=".", markersize=3)
        ax1.set_ylabel('AVG JCT (sec)', labelpad=1.0)
        if folder_prefix == f"s-alpha-zero-fixedgbs-pknob":
            # A
            plt.yticks([3750, 4000, 4250])
            ax1.set_ylim(3550, 4350)
            ax1.set_xlim(0, 5)
            ax1.set_xlabel('α', labelpad=-2.0)
            ax1.plot([0, 0], [3590, 3610], marker=[(-1, -1), (1, 1)], markersize=6, linestyle="none", clip_on=False, color='black')
            ax2 = ax1.twinx()
            ax2.set_ylabel('Fairness', labelpad=-0.01)
            ax2.set_ylim(0, 10)
            l2, = ax2.plot(xs, fairnesses, color = 'red', label='Fairness', linewidth=1, marker=".", markersize=3)
            ax1.legend(loc='upper right', handles=[l1, l2], fontsize=8, ncols=2, framealpha=1)   
            plt.gcf().set_size_inches(3.5, 2)
            plt.tight_layout()
            plt.savefig(f'../images/fig3_jct_fair_tradeoff/alpha-{trace_name}-0to5-0.25.pdf', dpi=300, bbox_inches='tight', pad_inches = 0.02)
            plt.clf()
        else:
            # coldzero A
            plt.yticks([3400, 3600, 3800])
            ax1.set_ylim(3250, 3850)
            ax1.set_xlim(0, 5)
            ax1.set_xlabel('α', labelpad=-2.0)
            ax1.plot([0, 0], [3310, 3330], marker=[(-1, -1), (1, 1)], markersize=6, linestyle="none", clip_on=False, color='black')
            ax2 = ax1.twinx()
            ax2.set_ylabel('Fairness')
            ax2.set_ylim(0, 5.5)
            ax2.set_yticks([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
            l2, = ax2.plot(xs, fairnesses, color = 'red', label='Fairness', linewidth=1, marker=".", markersize=3)
            ax1.legend(loc='upper right', handles=[l1, l2], fontsize=8, ncols=2, framealpha=1)   
            plt.gcf().set_size_inches(3.5, 2)
            plt.tight_layout()
            plt.savefig(f'../images/fig3_jct_fair_tradeoff/alpha-{trace_name}-0to5-0.25-coldzero.pdf', dpi=300, bbox_inches='tight', pad_inches = 0.02)


        