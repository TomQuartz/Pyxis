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
    colors = {1: 'red', 2: 'blue', 3: 'green'}
    labels = {1: '4', 2: '8', 3: '16'}
    kv_list = []
    table_id_list = []
    value_size_list = []
    order_list = []
    res = read(arg[1])
    for i in res:
        try:
            if i[0] == "kv":
                kv_list.append(int(i[-1]))
            elif i[0] == "table_id":
                table_id_list.append(int(i[-1]))
            elif i[0] == "order":
                order_list.append(int(i[-1]))
        except:
            pass
    
    assert len(kv_list) == 16
    assert len(table_id_list) == 16
    assert len(order_list) == 16
    
    value_size = [64, 256, 1024, 4096, 16384, 49152]
    for i in range(len(kv_list)):
        value_size_list.append(kv_list[i] * value_size[table_id_list[i] - 1])
    for i in range(len(value_size_list)):
        value_size_list[i] = np.log2(value_size_list[i]) / 2
        order_list[i] = np.log10(order_list[i])

    plt.cla()
    plt.title('16 type')
    plt.xlabel('Log10 order/cycle')
    plt.ylabel('Log4 value_size/byte')
    plt.scatter(order_list, value_size_list, marker='x', color='red')
    plt.grid()
    # plt.legend()
    file_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(file_path, '16type.png')
    # print(file_path)
    plt.savefig(file_path)
