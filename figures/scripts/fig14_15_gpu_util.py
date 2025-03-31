import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys

file_names = ["alpha-0", "alpha-1", "alpha-10", "a-srsf", "a-tiresias", "a-themis"]

make_small_size_graph = True
cluster_size = 40.0

for workload in ['a', 'b']:
    print_flag = True
    file_list = [
        f"../../logs/{workload}/{workload}-alpha-0.util", \
        f"../../logs/{workload}/{workload}-alpha-1.util", \
        f"../../logs/{workload}/{workload}-alpha-10.util", \
        f"../../logs/{workload}/{workload}-a-srsf.util", \
        f"../../logs/{workload}/{workload}-a-tiresias.util", \
        f"../../logs/{workload}/{workload}-a-themis.util"
    ]

    if workload == 'a':
        num_job = 100
    else:
        num_job = 50

    max_t = 0
    for f in file_list:
        try:
            with open(f, "r") as file:
                while True:
                    line = file.readline()
                    if not line:
                        break
                    segs = line.rstrip().split(',')
                    if int(segs[0]) > max_t:
                        max_t = int(segs[0])
        except:
            pass
    max_t = int(int(max_t * 1.05))
    max_t = float((float(max_t)/3600 * 1.05))

    print(f"{workload}, Max Timeline: {max_t}, Num Jobs: {num_job}")

    if make_small_size_graph:
        x_axis = [0, int(int(max_t)*0.5), int(int(max_t)*0.5) * 2]
    else:
        x_axis = [0, int(max_t*0.25), int(max_t*0.5), int(max_t*0.75), max_t]

    for ind, log_file in enumerate(file_list):
        try:
            with open(log_file, "r") as l:
                before_loop_time = 0.0
                alloc = 0
                frag_history = []
                queued = False
                queue_frag_history = []
                alloc_graph = []
                
                while True:
                    line = l.readline()
                    if not line:
                        break
                    loop_time = int(line.rstrip().split(",")[0])
                    loop_time = float(line.rstrip().split(",")[0])/3600.0
                    time_duration = loop_time - before_loop_time

                    if len(alloc_graph) != 0 and alloc_graph[-1][2] == queued and alloc_graph[-1][3] == alloc / cluster_size * 100.0:
                        alloc_graph[-1][1] = loop_time
                    else:
                        alloc_graph.append([before_loop_time, loop_time, queued, alloc / cluster_size * 100.0])
                    
                    frag_history.append([time_duration, cluster_size - alloc])
                    if queued:
                        queue_frag_history.append([time_duration, cluster_size - alloc])

                    if "True" in line.rstrip().split(",")[-1]:
                        queued = True
                    else:
                        queued = False
                                    
                    alloc = int(line.rstrip().split(",")[1])
                    before_loop_time = loop_time

                fh = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                # [time_duration, 40.0-alloc]
                for f in frag_history:
                    fh[0] += f[0]
                    fh[1] += f[1] * f[0]
                    fh[4] += (cluster_size - f[1]) * f[0]
                # [time_duration, 40.0-alloc]
                for f in queue_frag_history:
                    fh[2] += f[0]
                    fh[3] += f[1] * f[0]

                if print_flag:
                    print(f"{workload}, Avg_frag, Avg_Q_frag, GPUTime, Avg_frag(%), Avg_Q_frag(%), GPUTime(%)")
                    print_flag = False
                if fh[0] == 0:
                    fh[0] = 1
                if fh[2] == 0:
                    fh[2] = 1
                print(f"{file_names[ind]}, {fh[1]/fh[0]:.3f}, {fh[3]/fh[2]:.3f}, {fh[4]:.2f}, {fh[1]/fh[0]/cluster_size*100:.2f}, {fh[3]/fh[2]/cluster_size*100:.2f}, {fh[4]*100/(cluster_size*fh[0]):.2f}")

                plt.clf()
                plt.grid(True, alpha=0.5, linewidth=0.75)
                
                before = 0
                line_width = 1.2
            
                for f in alloc_graph:
                    blt = f[0] # before_loop_time
                    lt = f[1] # loop_time
                    qd = f[2] # queued
                    ar = f[3] # alloc_rate
                    plt.fill_between([blt, lt], ar, 0, color = 'mistyrose', zorder=1)
                    plt.hlines(ar, blt, lt, color='r', linestyles='solid', linewidth=line_width, zorder=10)
                    if qd == True and ar < 100:
                        plt.fill_between([blt, lt], ar, 100, color = 'lightcoral', zorder=5)
                    if before < ar:
                        plt.vlines(blt, before, ar, color='r', linestyles='solid', linewidth=line_width, zorder=10)
                    else:
                        plt.vlines(blt, ar, before, color='r', linestyles='solid', linewidth=line_width, zorder=10)
                    before = ar
                plt.vlines(alloc_graph[-1][1], before, 0, color='r', linestyles='solid', linewidth=line_width, zorder=10)
                
                plt.xlabel('Time (h)', fontsize=11, labelpad=0)
                plt.ylabel('Utilized GPUs (%)', fontsize=11, labelpad=0)
                plt.xlim(0, max_t)
                plt.ylim(0, 105)
                plt.xticks(x_axis)
                plt.yticks([0, 25, 50, 75, 100])

                if make_small_size_graph:
                    plt.gcf().set_size_inches(3, 2.1) # w, h # default
                    plt.tight_layout()
                    plt.savefig(f"../images/fig14_15_gpu_util/small-frag-merged-{workload}-{file_names[ind]}-hour.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)
                else:
                    plt.gcf().set_size_inches(7, 2.5) # w, h
                    plt.tight_layout()
                    plt.savefig(f"../images/fig14_15_gpu_util/frag-merged-{workload}-{file_names[ind]}.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)
        except:
            pass

