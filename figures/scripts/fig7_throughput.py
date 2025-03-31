import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import seaborn as sns

data = [
#resnext
[145.455, 287.385, 567.054, 564.496, 1125.376, 1110.536, 0, 0],
[147.534, 285.423, 564.154, 557.595, 1076.015, 1055.496, 0, 0],
[0, 0, 0, 0, 0, 0, 0, 0,], #filler
#resnet50
[54.561, 105.233, 206.655, 205.298, 400.801, 394.410, 751.056, 748.122],
[55.741, 108.977, 214.134, 215.363, 420.303, 416.067, 813.912, 799.136],
[0, 0, 0, 0, 0, 0, 0, 0,], #filler
#vgg19
[30.858, 55.107, 96.683, 94.843, 176.190, 152.355, 326.219, 299.729],
[30.730, 57.528, 108.849, 107.742, 178.687, 171.866, 259.515, 247.603],
[0, 0, 0, 0, 0, 0, 0, 0,], #filler
#GNMT
[44.220, 81.082, 157.657, 157.665, 305.311, 287.823, 0, 0],
[42.887, 79.012, 145.321, 144.393, 264.033, 259.859, 0, 0],
[0, 0, 0, 0, 0, 0, 0, 0,], #filler
#BERTLARGE
[2.157, 4.008, 7.720, 7.770, 15.132, 15.096, 29.691, 28.135],
[2.235, 4.432, 8.519, 8.426, 16.031, 16.543, 24.746, 23.543],
[0, 0, 0, 0, 0, 0, 0, 0,], #filler
#GPT2
[1.578, 2.902, 5.625, 5.603, 10.769, 10.827, 21.405, 21.215],
[1.610, 3.039, 5.999, 6.017, 11.753, 11.615, 23.028, 21.700],
[0, 0, 0, 0, 0, 0, 0, 0,], #filler
]

yticks = [
[0, 200, 400, 600, 800, 1000, 1200],
[0, 150, 300, 450, 600, 750, 900],
[0, 70, 140, 210, 280, 350, 420],
[0, 60, 120, 180, 240, 300, 360],
[0, 6, 12, 18, 24, 30, 36],
[0, 4, 8, 12, 16, 20, 24],
]


xs = ["1-1", "2-1", "4-1", "4-2", "8-2", "8-4", "16-4", "16-8"]
models = ["(a) ResNeXt29", "(b) ResNet50", "(c) VGG19", "(d) GNMT", "(e) BERT-large", "(f) GPT-2 small",]
ylabel_list = ["Througput (Images/Sec)", "Througput (Images/Sec)", "Througput (Images/Sec)", "Througput (Images/Sec)", "Througput (Images/Sec)", "Througput (Batch/Sec)", "Througput (Batch/Sec)",]
index = np.arange(8)
bar_width = 0.3

for i in range(6):
    print('plotting {}th model {}'.format(i, models[i]))
    plt.rc('axes', axisbelow=True)
    plt.grid(True, alpha=0.5, linewidth=0.75)
    plt.subplot(2, 3, i+1)
    plt.rc('axes', axisbelow=True)
    plt.grid(True, alpha=0.5, linewidth=0.75)
    plt.title(models[i], fontdict = {'size':27, 'family':'Times New Roman'}, y=-0.48)
    if i == 0 or i == 3:
        plt.ylabel("Throughput (Batch/Sec)", fontsize=14)
    plt.bar(index+0.5*bar_width-0.1, data[i*3+1], bar_width, color='C1', label='FEDeS')
    plt.bar(index+1.5*bar_width-0.1, data[i*3], bar_width, color='C2', label='ARF')
    plt.xticks(np.arange(0.2, 7.9 + bar_width-0.1, 1), xs, fontsize=16, rotation=45)
    plt.yticks(yticks[i], fontsize=16)
    plt.ylim(yticks[i][0], yticks[i][-1]*1.1)

    plt.gcf().set_size_inches(13, 7) # w, h

    plt.tight_layout(pad=0.2, rect=[0, 0, 1, 1])
    if i == 0:
        plt.legend(loc='upper left', fontsize=16, frameon=True, ncol=1)

filename = '../images/fig7_throughput/throughput.pdf'
plt.savefig(filename, dpi=600)
