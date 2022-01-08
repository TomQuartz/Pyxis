import os
import sys
import re
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

stop_list = ['\n']
arg = sys.argv

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


if __name__ == "__main__":
    n = 64
    m = 8
    num_types = 4
    p = 20
    out1 = 1
    log_tput = 0
    smooth = 0
    optimal = 0
    num_curves = 4
    if optimal:
        num_curves += 1
    colors = {1: 'black', 2: 'blue', 3: 'limegreen', 4: 'darkorange', 5: 'red'}
    labels = {1: 'disaggr', 2: 'kayak', 3: 'only C', 4: 'only S', 5: 'optimal'}
    line_styles = {1: 'solid', 2: 'dashed', 3: 'dashdot', 4: 'dotted', 5: (5, (10, 5))}
    line_widths = {1: 1.2, 2: 1.6, 3: 1.6, 4: 1.6, 5: 1.5}
    lat = {i: [] for i in range(1, num_curves + 1)}
    tput = {i: [] for i in range(1, num_curves + 1)}
    lat_all = []
    tput_all = []
    res = read(arg[1])
    for i in res:
        try:
            if i[0] == ">>>":
                lat_all.append(float(i[-1][6:]) / 1000)
            elif i[0] == "PUSHBACK":
                tput_all.append(float(i[-1]) / 1000)
        except:
            pass
    
    lb_out = [1, 2, 4, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256]
    kayak_out = [1, 2, 4, 6, 8, 12, 16, 24, 32, 48]
    only_c_out = [1, 2, 4, 8, 16, 32]
    only_s_out = [1, 2, 4, 6, 8, 12, 16, 24, 32]
    lb_optimal_out = [1, 2, 4, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256]
    if out1:
        lb_out += [2, 4, 6]
        kayak_out += [2, 4, 6]
        only_c_out += [4, 6]
        only_s_out += [2, 4, 6, 7]
        lb_optimal_out += [2, 4, 6]

    split_pos = [0, len(lb_out), len(kayak_out), len(only_c_out), len(only_s_out)]
    if optimal:
        split_pos.append(len(lb_optimal_out))
    for i in range(1, num_curves + 1):
        split_pos[i] += split_pos[i-1]
    for i in range(1, num_curves + 1):
        lat[i] = lat_all[split_pos[i-1]: split_pos[i]]
        tput[i] = tput_all[split_pos[i-1]: split_pos[i]]

    plt.cla()
    plt.title('{}C{}S {}type'.format(n, m, num_types))
    if log_tput:
        plt.xlabel('Log10 Throughput/KOps')
    else:
        plt.xlabel('Throughput/MOps')
    plt.ylabel('Latency/us')
    # plt.xlim(1.0, 3.5)
    plt.ylim(0, 1500)
    for j in range(1, num_curves + 1):
        if log_tput:
            for i in range(len(tput[j])):
                tput[j][i] = np.log10(tput[j][i])
        else:
            for i in range(len(tput[j])):
                tput[j][i] = tput[j][i] / 1000
        tput_s, lat_s = tput[j], lat[j]
        if smooth:
            model = make_interp_spline(tput[j], lat[j])
            tput_s = np.linspace(min(tput_s), max(tput_s), 300)
            lat_s = model(tput_s)            
        plt.plot(tput_s, lat_s, label=labels[j], linestyle=line_styles[j], color=colors[j], lw=line_widths[j])
    plt.grid()
    plt.legend()
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1][:-4]+'.png')
    # print(file_path)
    plt.savefig(file_path)
