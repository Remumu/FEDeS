import sys
import random

input_file = sys.argv[1]

models = ["GPT2-SMALL", "BERTLARGE", "ResNet50", "VGG19-I", "ResNeXt", "GNMT", "duration"]

# horovod / DDP iter time
iter_times = [[1.268, 1.367, 1.399, 1.490, 1.499],
              [1.854, 2.013, 2.085, 2.149, 2.154],
              [1.173, 1.223, 1.270, 1.277, 1.348],
              [1.037, 1.169, 1.355, 1.453, 1.584],
              [0.880, 0.889, 0.911, 0.907, 1.000], 
              [1.447, 1.580, 1.626, 1.664, 1.000], 
              [1.000, 1.000, 1.000, 1.000, 1.000]]

num_gpus = [1, 2, 4, 8, 16]

su = []
m = []
g = []
du = []

with open(input_file) as trace:
    while True:
        line = trace.readline()
        if not line:
            break
        record = line.rstrip().split()
        su.append(int(record[0]))
        m.append(record[1])
        g.append(int(record[2]))
        du.append(int(record[3]))

for i, model in enumerate(m):
    if g[i] == 16:
        new_model = models[random.randint(0,3)]
    else:
        new_model = models[random.randint(0,5)]
    old_iter_time = iter_times[models.index(m[i])][num_gpus.index(g[i])]
    new_iter_time = iter_times[models.index(new_model)][num_gpus.index(g[i])]
    new_iteration = int(du[i]*old_iter_time/new_iter_time)
    if new_iteration == 0:
        new_iteration = 1
    m[i] = new_model
    du[i] = new_iteration

for i, s in enumerate(su):
    print("{} {} {} {}".format(s, m[i], g[i], du[i]))