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
    colors = {1: 'darkorange', 2: 'purple'}
    labels = {1: 'Disaggr', 2: 'Disaggr (elastic)'}
    line_styles = {1: 'solid', 2: ':'}
    line_widths = {1: 2.4, 2: 2.4}
    num_curves = 2
    lat = {i: [] for i in range(1, num_curves + 1)}
    tput = {i: [] for i in range(1, num_curves + 1)}
    cores = []
    data_idx = 0
    res = read(arg[1])
    for i in res:
        try:
            if i[0].strip() in {'lb:', 'kayak:', 'only'} or i[1].strip() == 'optimal:':
                data_idx += 1
            if i[0].strip() == ">>>":
                lat[data_idx].append(float(i[-1][6:]) / 1000)
            elif i[0].strip() == "PUSHBACK" or i[0].strip() == "Throughput":
                tput[data_idx].append(float(i[-1]) / 1000)
            elif i[0].strip() == "Core" and data_idx == 2:
                cores.append(int(i[-1].strip()[:-1]))
        except:
            pass

    plt.cla()
    fig, ax1 = plt.subplots()
    plt.xlabel('Throughput (KOps)')
    y_ticks = [i * 300 for i in range(6)]
    ax1.set_yticks(y_ticks)
    ax1.set_ylabel('99% Latency ('+chr(956)+'s)')
    ax1.set_ylim(0, 1500)
    # plt.xlim(1.0, 4.0)
    plt.xlim(10, 1e4)
    plt.xscale('log')
    plt.xlabel('Throughput (KOps)')
    for j in range(1, num_curves + 1):    
        ax1.plot(tput[j], lat[j], label=labels[j], linestyle=line_styles[j], lw=line_widths[j], color=colors[j])
    ax2 = ax1.twinx()
    ax2.set_ylabel('# of Cores on App Servers')
    ax2.set_ylim(0, 144)
    y2_ticks = [0, 32, 64, 96, 128]
    ax2.set_yticks(y2_ticks)
    ax2.plot(tput[2], cores, label='# of Cores', color='red', linestyle='--')
    ax1.grid()
    fig.legend(loc='upper center')
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1]+'.png')
    # print(file_path)
    plt.savefig(file_path)
