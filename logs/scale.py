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
    colors = {1: 'red', 2: 'blue', 3: 'green', 4: 'darkviolet', 5: 'black'}
    labels = {1: 'out=8', 2: 'out=12', 3: 'out=16', 4: 'out=24', 5: 'out=32'}
    tput_all = []
    res = read(arg[1])
    for i in res:
        try:
            if i[0] == "PUSHBACK":
                tput_all.append(float(i[-1])/1e6)
        except:
            pass
    tput = {i: [] for i in range(5)}
    for i in range(5):
        tput[i] = tput_all[i*4: (i+1)*4]
    x_ticks = [2, 4, 6, 8]
    plt.cla()
    plt.title('scale')
    plt.xlabel('num_app_servers')
    plt.xticks(x_ticks)
    plt.ylabel('Tput (MOps)')
    for i in range(5):
        plt.scatter(x_ticks, tput[i], label=labels[i+1])
    plt.legend()
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1][:-4]+'.png')
    # print(file_path)
    plt.savefig(file_path)
