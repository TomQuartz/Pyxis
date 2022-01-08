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
    partition = []
    for i in res:
        try:
            if i[0] == "PUSHBACK":
                d = 0
                t[d].append(float(i[-1]) / 1e6)
            elif i[0] == 'partition':
                partition.append(float(i[-1]))
        except:
            pass
    partition = partition[:-1]

    plt.cla()
    fig, ax1 = plt.subplots()
    ax1.set_xlabel('Partition (%)')
    # ax1.set_ylim(0.0, 100.0)
    ax1.plot(partition, t[0], color='red', linestyle='solid')
    ax1.set_ylabel('Throughput (MOps)')
    ax1.set_xlim(0, 100)
    # fig.legend()
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, arg[1]+'.png')
    # print(file_path)
    plt.savefig(file_path)
