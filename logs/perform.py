import os
import sys
import re
import matplotlib.pyplot as plt
import numpy as np

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
    n = 32
    core1 = 1
    log_tput = 1
    num_curves = 4
    labels = {1: 'disaggr', 2: 'kayak', 3: 'only C', 4: 'only S'}
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
                tput_all.append(float(i[-1]) / 1e6)
        except:
            pass
    
    lb_out = [1, 2, 4, 8, 16, 32, 64]
    kayak_out = [1, 2, 4]
    only_c_out = [1, 2, 4]
    only_s_out = [1, 2, 4, 8]
    if core1:
        lb_out += [1, 2, 4]
        kayak_out += [1, 2, 4]
        only_c_out += [1, 2, 4]
        only_s_out += [1, 2, 4]

    split_pos = [0, len(lb_out), len(kayak_out), len(only_c_out), len(only_s_out)]
    for i in range(1, 5):
        split_pos[i] += split_pos[i-1]
    for i in range(1, num_curves + 1):
        lat[i] = lat_all[split_pos[i-1]: split_pos[i]]
        tput[i] = tput_all[split_pos[i-1]: split_pos[i]]
    plt.cla()
    plt.title('{}C1S/2type'.format(n))
    if log_tput:
        plt.xlabel('Log10 Throughput/MOps')
    else:
        plt.xlabel('Throughput/MOps')
    plt.ylabel('Latency/us')
    for j in range(1, num_curves + 1):
        if log_tput:
            for i in range(len(tput[j])):
                tput[j][i] = np.log10(tput[j][i])
        plt.plot(tput[j], lat[j], label=labels[j])
    plt.legend()
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1][:-4]+'.png')
    # print(file_path)
    plt.savefig(file_path)
