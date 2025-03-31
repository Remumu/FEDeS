import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys

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

    max_t = 0
    for i, f in enumerate(file_list):
        try:
            with open(f, "r") as file:
                line = file.readline()
                while True:
                    line = file.readline()
                    if not line:
                        break
                    segs = line.rstrip().split(',')
                    if i < 3:
                        jct = int(segs[6])
                        running = int(segs[7])
                    else:
                        jct = int(segs[3]) / 1000
                        running = int(segs[4]) / 1000
                    if int(jct) - int(running) > max_t:
                        max_t = int(jct) - int(running)
        except:
            pass
    max_t = int(int(max_t * 1.05))
    max_t = int(int(float(max_t)/3600.0 * 1.05))

    for i, f in enumerate(file_list):
        try:
            with open(f, "r") as file:
                line = file.readline()
                plt.clf()
                plt.rc('axes', axisbelow=True)
                plt.grid(True, alpha=0.5, linewidth=0.75)   
                # plt.ylabel('Time (s)', fontsize=16)
                plt.ylabel('Time (h)', fontsize=16)
                plt.xlabel('Job #', fontsize=16)
                while True:
                    line = file.readline()
                    if not line:
                        break
                    m = file_names[i]
                    segs = line.rstrip().split(',')

                    s = 0
                    q = 0

                    if i < 3:
                        job_n = int(segs[0][4:]) - 1
                        submit = int(segs[3])
                        scheduled = int(segs[5])
                        jct = int(segs[6])
                        running = int(segs[7])
                        submit = float(segs[3])/3600.0
                        scheduled = float(segs[5])/3600.0
                        jct = float(segs[6])/3600.0
                        running = float(segs[7])/3600.0
                    else:
                        job_n = int(segs[0][3:]) - 1
                        submit = int(segs[1]) / 1000
                        scheduled = int(segs[2]) / 1000
                        jct = int(segs[3]) / 1000
                        running = int(segs[4]) / 1000
                        submit = float(segs[1]) / 1000.0 / 3600.0
                        scheduled = float(segs[2]) / 1000.0 / 3600.0
                        jct = float(segs[3]) / 1000.0 / 3600.0
                        running = float(segs[4]) / 1000.0 / 3600.0
                    if float(submit) < float(scheduled):
                        s = (float(scheduled) - float(submit))
                        plt.bar(job_n, s, color='C0')
                    if float(scheduled) - float(submit) + float(running) < float(jct):
                        q = float(jct) - (float(scheduled) - float(submit) + float(running))
                        plt.bar(job_n, q, bottom=s, color='C1') 
                        
                # Dummy plots for making legend
                plt.plot([], [], label='Initial queuing', color='C0', linestyle='solid', linewidth=1)             
                plt.plot([], [], label='Queuing\nby preemption', color='C1', linestyle='solid', linewidth=1)

                plt.xlim(0, num_job+2)
                plt.ylim(0, max_t * 1.05)
                plt.xticks([0, num_job//2, num_job], fontsize=16)
                plt.yticks([0, int(max_t/2), int(max_t/2)*2], fontsize=16)
               

                if m == "alpha-0":
                    plt.legend(loc='upper right', frameon=True, fontsize=13.5, framealpha=1)

                plt.gcf().set_size_inches(4, 2.5) # w, h
                plt.tight_layout()
                n = file_names[i]
                plt.savefig(f"../images/fig12_13_queuing/qy-{workload}-{n}-hour.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)
        except:
            print(f"{f} not found")