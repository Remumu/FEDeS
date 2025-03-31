import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

data = [
#resnext
[1.000, 1.000, 1.000, 0.000],
[1.055, 1.003, 1.000, 0.000],
[0.000, 1.043, 1.025, 0.000],
[0.000, 0.000, 1.043, 0.000],
[0.000, 0.000, 0.000, 0.000],
#resnet50
[1.000, 1.000, 1.000, 1.000],
[1.057, 1.021, 1.013, 1.043],
[0.000, 1.028, 1.046, 1.037],
[0.000, 0.000, 1.098, 1.057],
[0.000, 0.000, 0.000, 1.108],
#vgg19
[1.000, 1.000, 1.000, 1.000],
[1.081, 1.070, 1.168, 1.330],
[0.000, 1.093, 1.274, 1.523],
[0.000, 0.000, 1.304, 1.635],
[0.000, 0.000, 0.000, 1.762],
#gnmt
[1.000, 1.000, 1.000, 0.000],
[1.072, 1.115, 1.118, 0.000],
[0.000, 1.164, 1.195, 0.000],
[0.000, 0.000, 1.306, 0.000],
[0.000, 0.000, 0.000, 0.000],
#bert-large
[1.000, 1.000, 1.000, 1.000],
[1.079, 1.039, 1.050, 1.342],
[0.000, 1.123, 1.085, 1.398],
[0.000, 0.000, 1.168, 1.452],
[0.000, 0.000, 0.000, 1.577],
#gpt-2-small
[1.000, 1.000, 1.000, 1.000],
[1.048, 1.012, 1.020, 1.052],
[0.000, 1.026, 1.036, 1.069],
[0.000, 0.000, 1.073, 1.086],
[0.000, 0.000, 0.000, 1.113],
]

hvd_perf = [
#resnext
[1.012, 1.002, 1.091, 0.000],
#resnet50
[0.954, 0.962, 0.959, 0.941],
#vgg19
[0.975, 0.865, 1.020, 1.332],
#gnmt
[1.060, 1.080, 1.190, 0.000],
#bert-large
[0.937, 0.910, 0.929, 1.104],
#gpt-2-small
[0.957, 0.941, 0.932, 0.931],
]


xs = ["2 GPUs", "4 GPUs", "8 GPUs", "16 GPUs"]
models = ["(a) ResNeXt29", "(b) ResNet50", "(c) VGG19", "(d) GNMT", "(e) BERT-large", "(f) GPT-2 small"]

index = np.arange(4)
bar_width = 0.15

for i in range(6):

    plt.rc('axes', axisbelow=True)
    plt.grid(True, alpha=0.5, linewidth=0.75, zorder=1)
    plt.subplot(2, 3, i+1)
    plt.rc('axes', axisbelow=True)
    plt.grid(True, alpha=0.5, linewidth=0.75, zorder=1)

    plt.title(models[i], y=-0.3, fontdict = {'size':25, 'family':'Times New Roman'})

    if i == 0 or i == 3:
        plt.ylabel("Norm. Throughput / LW", fontsize=16)
    plt.bar(index+0*bar_width, data[i*5], bar_width, color='C0', label='1 phase', zorder=10)
    plt.bar(index+1*bar_width, data[i*5+1], bar_width, color='C1', label='2 phase', zorder=10)
    plt.bar(index+2*bar_width, data[i*5+2], bar_width, color='C2', label='4 phase', zorder=10)
    plt.bar(index+3*bar_width, data[i*5+3], bar_width, color='C3', label='8 phase', zorder=10)
    plt.bar(index+4*bar_width, data[i*5+4], bar_width, color='C4', label='16 phase', zorder=10)

    for perf_idx in range(0, len(hvd_perf[i])):
        hvd_dat = [0, 0, 0, 0]
        hvd_dat[perf_idx] = hvd_perf[i][perf_idx]
        if perf_idx == 0:
            plt.bar(index+(perf_idx+2)*bar_width, hvd_dat, bar_width, color='C5', label='ARF', zorder=10)
        else:
            plt.bar(index+(perf_idx+2)*bar_width, hvd_dat, bar_width, color='C5', zorder=10)

    plt.xticks([0.2, 1.3, 2.4, 3.5], xs, fontsize=13)
    plt.yticks([0, 0.5, 1, 1.5, 2], fontsize=14)
    plt.ylim(0, 2)
    plt.gcf().set_size_inches(13, 7) # w, h
    plt.tight_layout(pad=0.2,rect=[0, 0, 1, 1])
lg = plt.legend(bbox_to_anchor=(1., 2.5), loc='lower right', fontsize=16, frameon=True, ncol=6)

filename = '../images/fig11_lw_throughput/lw_throughput.pdf'
plt.savefig(filename, dpi=600, bbox_inches='tight')
