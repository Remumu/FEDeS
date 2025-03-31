import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sklearn.preprocessing
import numpy as np
import sys
import json
import random

vc_list = [
    '11cb48', '6214e9', '6c71a0', 'b436b2', 'ee9e8c', \
    '103959', '0e4a51', '2869ce', '7f04ca', 'e13805', \
    'ed69ec', '51b7ef', '925e2b', '795a4c', '23dbec'
]

# A ee9e8c
# B 2869ce

for vc in vc_list:
    job_ids = []
    submitted_times = []
    num_of_gpus = []
    durations = []

    sd_job_ids = []
    sd_submitted_times = []
    sd_num_of_gpus = []
    sd_durations = []

    events = dict()
    with open(f"vc_durations/{vc}-duration.trace", "r") as f:
        job_id = 0
        while True:
            line = f.readline()
            if not line:
                break
            job_ids.append(job_id)
            submitted_time, _, num_of_gpu, duration = line.rstrip().split()
            submitted_time = int(submitted_time)
            num_of_gpu = int(num_of_gpu)
            duration = int(duration)

            # duration filtering
            if 1800 <= duration <= 24 * 3600:
                submitted_times.append(submitted_time)
                num_of_gpus.append(num_of_gpu)
                durations.append(duration)
                job_id += 1

    # duration scale down
    durations = list(sklearn.preprocessing.minmax_scale(durations, feature_range=(900, 7200), axis=0, copy=True))
    
    # for scaled down trace
    # submitted_time scale down
    # expected_last_submitted_time = 8 # A
    expected_last_submitted_time = 11 # B
    last_submitted_time = max(submitted_times)
    if last_submitted_time == 0:
        last_submitted_time = 1
    submitted_times = [
        int(
            submitted_time / last_submitted_time * (3600 * expected_last_submitted_time)
        ) for submitted_time in submitted_times
    ]

    # num_jobs scale down
    # expected_num_jobs = 100 # A
    expected_num_jobs = 50 # B
    # expected_num_jobs = len(submitted_times)
    interval = len(submitted_times) // expected_num_jobs
    min_submitted_time = submitted_times[0]
    for k in range(expected_num_jobs):
        num = 0 + k * interval
        sd_submitted_times.append(submitted_times[num] - min_submitted_time)
        sd_num_of_gpus.append(num_of_gpus[num])
        sd_durations.append(int(durations[num]))

    # plot gpu load
    for i, submitted_time in enumerate(sd_submitted_times):
        if submitted_time in events:
            events[submitted_time] += num_of_gpu
        else:
            events[submitted_time] = num_of_gpu

        finished_time = submitted_time + sd_durations[i]
        
        if finished_time in events:
            events[finished_time] -= num_of_gpu
        else:
            events[finished_time] = -1 * num_of_gpu
    times = []
    loads = []
    gpu_load = 0
    for k in sorted(events.keys()):
        gpu_load += events[k]
        times.append(k)
        loads.append(gpu_load)
    plt.clf()
    plt.plot(times, loads, 'g', label='GPU load')
    plt.tight_layout()
    # plt.savefig(f"filtered_durations" \
    # f"/gpu_load/{vc}-gpu-load.pdf", dpi=300, bbox_inches='tight', pad_inches = 0.02)

    # save scale downed trace
    with open(f"filtered_durations/{vc}-filtered-duration.trace", "w") as f:
        for i in range(len(sd_submitted_times)):
            f.write(f"{sd_submitted_times[i]} duration {sd_num_of_gpus[i]} {sd_durations[i]}\n")
