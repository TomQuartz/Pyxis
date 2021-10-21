import os
import matplotlib.pyplot as plt
import numpy as np

NUM_TENANT = 8


def aggregate_tput(tput, num_tenant=NUM_TENANT):
    real_tput = []
    i = 0
    while i < len(tput):
        real_tput.append(sum(tput[i: i + num_tenant]))
        i += num_tenant
    return real_tput


def read_kayak_data(num_type, s):
    filepath = DATE + '_' + EXPERIMENT + '/' + str(s) + '/' + \
               'kayak_type{}.log'.format(num_type)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.readlines()
    latency, tput = [], []
    for line in content:
        if line.find('lat99') != -1:
            lat = line.strip().split()
            lat = float(lat[-1][6:])
            latency.append(lat)
        elif line.find('Throughput') != -1:
            tp = line.strip().split()
            tp = float(tp[-1])
            tput.append(tp)
    # tput = aggregate_tput(tput)
    return latency, tput


def read_ours_data(num_type, max_out, s):
    filepath = DATE + '_' + EXPERIMENT + '/' + str(s) + '/' + \
               'ours_type{}_out{}.log'.format(num_type, max_out)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.readlines()
    latency, tput = [], []
    partition = []
    for line in content:
        if line.find('lat99') != -1:
            lat = line.strip().split()
            lat = float(lat[-1][6:])
            latency.append(lat)
        elif line.find('Throughput') != -1:
            tp = line.strip().split()
            tp = float(tp[-1])
            tput.append(tp)
        elif line.find('partition = ') != -1:
            part = line.strip().split()
            part = int(part[-1])
            partition.append(part)
    # tput = aggregate_tput(tput)
    max_tput = max(tput)
    idx = tput.index(max_tput)
    lat_tp = latency[idx]
    part_tp = partition[idx]
    return lat_tp, max_tput, part_tp


def read_bimodal_data(num_type, max_out, i1, i2):
    filepath = 'try8/type{}_out{}_i1{}_i2{}.log'.format(num_type, max_out, i1, i2)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.readlines()
    pos = []
    for i in range(len(content)):
        line = content[i]
        if line.find('Kayak') != -1:
            pos.append(i)
        elif line.find('bimodal_rpc') != -1:
            pos.append(i)
    lat = [[] for i in range(len(pos))]
    tput = [[] for i in range(len(pos))]
    bi = [[] for i in range(len(pos))]
    rpc = [[] for i in range(len(pos))]
    for i in range(len(pos)):
        if i < len(pos) - 1:
            rg = range(pos[i], pos[i+1])
        else:
            rg = range(pos[i], len(content))
        for j in rg:
            line = content[j]
            if line.find('rdtsc') != -1:
                l = line.strip().split()
                lat[i].append(float(l[-7]))
                tput[i].append(float(l[-5]))
                if int(l[-3]) == 0:
                    bi[i].append(0.9)
                else:
                    bi[i].append(0.1)
                rpc[i].append(float(l[-1][1:-1]) / 100)

    return lat, tput, bi, rpc


def get_max_tput(tput):
    tput_max = []
    min_len = len(tput[0])
    for i in tput:
        min_len = min(min_len, len(i))
    for i in range(min_len):
        max_tmp = tput[1][i]
        for j in range(2, len(tput)):
            max_tmp = max(max_tmp, tput[j][i])
        tput_max.append(max_tmp)
    return tput_max


def draw_bimodal():
    i1 = 960000000
    i2 = 3840000000
    lat, tput, kv_rate, rpc = read_bimodal_data(2, 8, i1, i2)

    for i in range(len(lat)):
        lat[i] = lat[i][1:]
        lat[i] = np.array(lat[i]) / 1e3
        lat[i] = lat[i].tolist()
        tput[i] = tput[i][1:]
        tput[i] = np.array(tput[i]) / 1e3
        tput[i] = tput[i].tolist()
        rpc[i] = rpc[i][1:]
        kv_rate[i] = kv_rate[i][1:]

    tput.append(get_max_tput(tput))

    M = max(tput[-1])
    m = min(tput[0][200:1100])
    print(M, m)
    print("multiple: {}".format(M / m))

    fig1, ax1 = plt.subplots()
    ax1.set_xlabel("time")
    ax1.set_ylim(-1, 2)
    ax1.set_ylabel("tput/Mops")
    ax1.plot(tput[0], label="tput-kayak")
    ax1.plot(tput[1], label="tput-rpc")
    ax2 = ax1.twinx()
    ax2.set_ylabel('rate')
    ax2.set_ylim(0.0, 3.0)
    ax2.plot(kv_rate[0], color='red', label='kv_type_rate')
    ax2.plot(rpc[0], color='green', label='rpc_rate')
    fig1.legend(loc='upper left')
    plt.show()

    # fig1, ax1 = plt.subplots()
    # ax1.set_xlabel("time")
    # ax1.set_ylim(-1, 2)
    # ax1.set_ylabel("tput/Mops")
    # ax1.plot(tput[1], label="tput-rpc{}".format(30))
    # ax2 = ax1.twinx()
    # ax2.set_ylabel('rate')
    # ax2.set_ylim(0.0, 3.0)
    # ax2.plot(kv_rate[1], color='red', label='kv_type_rate')
    # fig1.legend(loc='upper left')
    # plt.show()

    fig1, ax1 = plt.subplots()
    ax1.set_xlabel("time")
    ax1.set_ylim(-200, 1000)
    ax1.set_ylabel("tail latency/\u03bcs")
    ax1.plot(lat[0], label="lat-kayak")
    ax1.plot(lat[1], label="lat-rpc{}".format(40))
    ax2 = ax1.twinx()
    ax2.set_ylabel('rate')
    ax2.set_ylim(0.0, 3.0)
    ax2.plot(kv_rate[0], color='red', label='kv_type_rate')
    ax2.plot(rpc[0], color='green', label='rpc_rate')
    fig1.legend(loc='upper left')
    plt.show()

    # fig1, ax1 = plt.subplots()
    # ax1.set_xlabel("time")
    # ax1.set_ylim(-200, 1000)
    # ax1.set_ylabel("tail latency/\u03bcs")
    # ax1.plot(lat[1], label="lat-rpc{}".format(40))
    # ax2 = ax1.twinx()
    # ax2.set_ylabel('rate')
    # ax2.set_ylim(0.0, 3.0)
    # ax2.plot(kv_rate[1], color='red', label='kv_type_rate')
    # fig1.legend(loc='upper left')
    # plt.show()


def draw_enum(s):
    num_types = [4]
    max_out_list = [1, 2, 4, 8, 16, 32]
    for t in num_types:
        latency_kayak, tput_kayak = read_kayak_data(t, s)
        latency_ours, tput_ours = [], []

        for out in max_out_list:
            lat, tput, part = read_ours_data(t, out, s)
            latency_ours.append(lat)
            tput_ours.append(tput)

        latency_kayak = np.array(latency_kayak) / 1e3
        latency_ours = np.array(latency_ours) / 1e3
        tput_kayak = np.array(tput_kayak) / 1e6
        tput_ours = np.array(tput_ours) / 1e6

        plt.cla()
        plt.xlabel("99% latency/\u03bcs")
        plt.ylabel("tput/Mops")
        # plt.xlim(0, 2000)
        plt.ylim(0.0, 2.2)
        plt.plot(latency_kayak, tput_kayak, label='kayak-{}type'.format(t), marker='o')
        plt.plot(latency_ours, tput_ours, label='ours-{}type'.format(t), marker='^')
        plt.legend(loc='upper left')
        # plt.show()
        file_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(file_path, DATE + '_' + EXPERIMENT, str(s),
                                 'type{}.png'.format(t))
        print(file_path)
        plt.savefig(file_path)


DATE = 'enum1'
EXPERIMENT = 'enumerate'


def main():
    for i in range(12,15):
        draw_enum(i)


if __name__ == '__main__':
    main()
