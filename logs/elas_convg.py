import os
import sys
import re
import matplotlib.pyplot as plt

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
    res = read(arg[1])
    num_tenant = 1
    output_factor = 10.
    tput = []
    cores = []
    for i in res:
        try:
            if i[0] == "rdtsc":
                d = 0
                tput.append(float(i[3].strip()) / 1e3)
                cores.append(int((i[7].strip().split(','))[-1]))
        except:
            pass
    # print(tput)
    # print(cores)
    time = [j / output_factor for j in range(0, len(tput))]

    end = len(time)
    # end = int(output_factor) * 3 + 1

    plt.cla()
    fig, ax1 = plt.subplots()
    x_ticks = [i * 5 for i in range(7)]
    plt.xticks(x_ticks)
    plt.xlim(0, 20)
    plt.xlabel('Time (s)')
    y1_ticks = [0., 1., 2., 3., 4.]
    ax1.set_yticks(y1_ticks)
    ax1.set_ylim(0.0, 3.2)
    ax1.set_ylabel('Throughput (MOps)')
    ax1.plot(time[:end], tput[:end], label='Throughput', color='black', linestyle='solid')
    ax2 = ax1.twinx()
    ax2.set_ylabel('# of Cores on App Servers')
    ax2.set_ylim(0, 144)
    y2_ticks = [0, 32, 64, 96, 128]
    ax2.set_yticks(y2_ticks)
    ax2.plot(time[:end], cores[:end], label='# of Cores', color='red', linestyle='--')
    fig.legend()
    ax1.spines['top'].set_color('none')
    ax2.spines['top'].set_color('none')
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1]+'.png')
    # print(file_path)
    plt.savefig(file_path)
