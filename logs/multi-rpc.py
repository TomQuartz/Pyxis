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
    output_factor = 10
    time = 5
    num_types = 4
    initial_ratio = 0
    rpc1 = []
    rpc2 = []
    rpc3 = []
    rpc4 = []
    res = read(arg[1])
    for i in res:
        try:
            if i[0] == 'rdtsc':
                if num_types == 2:
                    rpc1.append(float(i[-2].strip()[-5:-1]) / 100)
                    rpc2.append(float(i[-1].strip()[:-1]) / 100)
                elif num_types == 4:
                    rpc1.append(float(i[-4].strip()[-5:-1]) / 100)
                    rpc2.append(float(i[-3].strip()[:-1]) / 100)
                    rpc3.append(float(i[-2].strip()[:-1]) / 100)
                    rpc4.append(float(i[-1].strip()[:-1]) / 100)
        except:
            pass
    if num_types == 2:
        start_pos = 0
        # start_pos = int(0.4 * len(rpc1))
        end_pos = start_pos + 10*output_factor + 1
        rpc1 = rpc1[start_pos:end_pos]
        rpc2 = rpc2[start_pos:end_pos]
        # r1 = np.linspace(0, rpc1[0], 15)
        # r2 = np.linspace(0, rpc2[0], 15)
        # y_noise = np.random.normal(scale=5, size=r1.size)
        # r1 += y_noise
        # y_noise = np.random.normal(scale=5, size=r2.size)
        # r2 += y_noise
        # rpc1 = np.append(r1, np.array(rpc1))
        # rpc2 = np.append(r2, np.array(rpc2))
    elif num_types == 4:
        end_pos = time * output_factor + 1
        rpc1 = rpc1[:end_pos]
        rpc2 = rpc2[:end_pos]
        rpc3 = rpc3[:end_pos]
        rpc4 = rpc4[:end_pos]
    time_slice = [j / output_factor for j in range(0, len(rpc1))]
    time_slice = np.array(time_slice)
    plt.ylim(0.0, 100.)
    plt.ylabel('rpc fraction (%)')
    plt.xlabel('time (s)')
    plt.plot(time_slice, rpc1, label='rpc1')
    plt.plot(time_slice, rpc2, label='rpc2')
    if num_types == 4:
        plt.plot(time_slice, rpc3, label='rpc3')
        plt.plot(time_slice, rpc4, label='rpc4')
    plt.legend()
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1][:-4]+'.png')
    plt.savefig(file_path)
