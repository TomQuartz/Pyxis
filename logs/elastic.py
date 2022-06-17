# %matplotlib inline
import os
import sys
import re
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
# import palettable

stop_list = ['\n']

def read(filename, split = " "):
    assert isinstance(filename, str)
    assert isinstance(split, str)
    return_list = []
    with open(filename, 'r') as f:
        tmpstr = f.readline()
        while(tmpstr):
            tmplist = re.split(split, tmpstr)
            for i in stop_list:
                try:
                    tmplist.remove(i)
                except:
                    pass
            return_list.append(tmplist)
            tmpstr = f.readline()
    return return_list

# constants
num_curves = 3
num_subfigs = 1
output_factor = 10.
freq = 2.4*1e9

# Read data
dir_name = "./logs/"
print(sys.argv)
if len(sys.argv) > 1:
    filename = f"elastic-{sys.argv[1]}.log"
else:
    filename = "elastic.log"
print(filename)
log_path = {1: dir_name+filename}
res = {j: None for j in range(1, num_subfigs+1)}
cores = {j: [64] for j in range(1, num_subfigs+1)}
tput = {j: [0] for j in range(1, num_subfigs+1)}
lat = {j: [10] for j in range(1, num_subfigs+1)}
time0 = {j: [0] for j in range(1, num_subfigs+1)}  # for tput and ratio
for j in range(1, num_subfigs+1):
    res[j] = read(log_path[j])
    for i in res[j]:
        try:
            if i[0] == "rdtsc":
                time0[j].append(float(i[1].strip()))
                tmp = i[-5].strip().split(',')
                cores[j].append(float(tmp[1]))
                tput[j].append(float(i[3].strip()))
                tmp = i[7].strip().split('(')
                lat[j].append(float(tmp[0]))
        except:
            pass
    tput[j] = np.array(tput[j]) / 1000
    time0_base = time0[j][0]
    time0[j] = np.array(time0[j]) 
    time0[j] -= time0_base
    time0[j] /= freq
    lat[j] = np.array(lat[j])
    cores[j] = np.array(cores[j])

# Set font and figure size
font_size = 28
# plt.rc('font',**{'size': font_size, 'family': 'Arial'})
# plt.rc('pdf',fonttype = 42)
# fig_size = plt.rcParams['figure.figsize']
fig_size = (12, 9)
fig, axes = plt.subplots(nrows=2, ncols=num_subfigs, sharex=True, 
                         gridspec_kw={'height_ratios':[4.5, 4]}, figsize=fig_size)
axes_twinx = []
for i in range(num_subfigs):
    axes_twinx.append(axes[0].twinx())
matplotlib.rcParams['xtick.minor.size'] = 4.
matplotlib.rcParams['xtick.major.size'] = 8.
matplotlib.rcParams['ytick.major.size'] = 6.
# plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.16, hspace=None)

# line setting

# x-axis setting
x_ticks = [5*i for i in range(5)]
x_label = 'Time (s)'

# y-axis setting
y1_bottom = [0]
y1_top = [4]
y1_ticks = [[1*i for i in range(5)]]
y1_label = 'Throughput (MOps)'
y2_ticks = [32*i for i in range(5)]
y2_label = '# of Cores on\nCompute Nodes'
y3_label = 'Normalized Latency'
y3_ticks = [5*i for i in range(5)]

# Plot x ticks and label
for j in range(num_subfigs):
    axes[1].set_xlabel(x_label)
    axes[1].set_xlim(left=0, right=20)
    axes[1].xaxis.set_ticks_position('bottom')
    axes[1].set_xticks(x_ticks)
    axes[0].get_xaxis().set_tick_params(direction='in')
    axes[1].get_xaxis().set_tick_params(direction='in', pad=6)
    axes_twinx[j].get_xaxis().set_tick_params(direction='in')

# Plot y ticks and label
for j in range(num_subfigs):
    axes[0].set_ylim(bottom=y1_bottom[j], top=y1_top[j])
    if j == 0:
        axes[0].set_ylabel(y1_label, labelpad=30)
        axes[1].set_ylabel(y2_label)
    if j == num_subfigs - 1:
        axes_twinx[j].set_ylabel(y3_label, color='red')
    axes[0].yaxis.set_ticks_position('left')
    axes[0].set_yticks(y1_ticks[j])
    axes[0].get_yaxis().set_tick_params(direction='in')
    axes_twinx[j].set_ylim(bottom=0, top=20)
    axes_twinx[j].yaxis.set_ticks_position('right')
    axes_twinx[j].set_yticks(y3_ticks)
    axes_twinx[j].get_yaxis().set_tick_params(labelcolor='red', direction='in')
    axes[1].set_ylim(bottom=0, top=144)
    axes[1].yaxis.set_ticks_position('left')
    axes[1].set_yticks(y2_ticks)
    axes[1].get_yaxis().set_tick_params(direction='in')

# Plot curves
for j in range(1, num_subfigs+1):
    line1,  = axes[0].plot(time0[j], tput[j], label='Throughput', color='black', linestyle='solid', lw=4.8, zorder=3)
    line2,  = axes[1].plot(time0[j], cores[j], label='# of Cores', color='royalblue', linestyle='solid', lw=5, zorder=1)
    line5 = axes_twinx[j-1].scatter(time0[j], lat[j], label='99%-tile Latency', color='r', s=10)

# Plot legend
# fig.legend(handles=[line1, line5, line2], handlelength=2.5, 
#            ncol=num_curves+1, loc='upper center', bbox_to_anchor=(0.495, 1), frameon=False, prop={'size':font_size})
axes[0].legend(handles=[line1, line5], handlelength=2, 
               ncol=1, loc='upper center', bbox_to_anchor=(0.76, 1.08), frameon=False, prop={'size':font_size})
axes[1].legend(handles=[line2], handlelength=2, 
               ncol=1, loc='upper center', bbox_to_anchor=(0.76, 1.08), frameon=False, prop={'size':font_size})

# Set top and right axes to none
for j in range(num_subfigs):
    axes[0].spines['top'].set_color('none')
    axes_twinx[j].spines['top'].set_color('none')
    axes[1].spines['top'].set_color('none')
    axes[1].spines['right'].set_color('none')

# Save the figure
file_path = './logs/Elastic_with_slo.png'
plt.savefig(file_path)