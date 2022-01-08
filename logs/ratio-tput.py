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
    # mov_avg = 0
    output_factor = 10.
    r = {i:[] for i in range(num_tenant)}
    t = {i:[] for i in range(num_tenant)}
    for i in res:
        try:
            if i[0] == "rdtsc":
                d = 0
                r[d].append(float(i[5]))
                t[d].append(float(i[3]))
        except:
            pass
    time_slice = {i: [j / output_factor for j in range(0, len(r[i]))] for i in range(num_tenant)}
    for i in range(len(t[0])):
        t[0][i] /= 1000
        r[0][i] /= 100

    # if mov_avg:
    #     for i in range(2, len(t[0]) - 2):
    #         t[0][i] = (t[0][i-1] + t[0][i] + t[0][i+1]) / 5

    end = len(time_slice[0])
    end = int(output_factor) * 3 + 1

    plt.cla()
    fig, ax1 = plt.subplots()
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Partition (%)')
    ax1.set_ylim(0.0, 100.0)
    ax1.plot(time_slice[0][:end], r[0][:end], label='partition', color='red', linestyle='--')
    ax2 = ax1.twinx()
    ax2.set_ylabel('Throughput (MOps)')
    # ax2.set_ylim(0.0, 2.0)
    ax2.plot(time_slice[0][:end], t[0][:end], label='tput', color='blue', linestyle='solid')
    fig.legend(loc='upper right')
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1]+'.png')
    # print(file_path)
    plt.savefig(file_path)
