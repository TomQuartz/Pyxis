import sys
import numpy as np
import random

arg = sys.argv
kv_list = [1, 2, 3, 4]
table_list = [1, 2]
log_kv_size_low, log_kv_size_high = 3, 9
log_order_low, log_order_high = 1, 3


def sample_type():
    kv = random.choice(kv_list)
    table_id = random.choice(table_list)
    # table: 64, 256, 1K, 4K, 16K, 48K
    log_kv_size = np.random.randint(log_kv_size_low, log_kv_size_high)
    if log_kv_size == 8:
        kv_size = 48 * 1024
    else:
        kv_size = 4 ** log_kv_size
    table_id = log_kv_size - 2
    log_order = np.random.uniform(log_order_low, log_order_high, 1)
    order = 120 * (10 ** log_order)
    if order < 2400:
        order = int(order / 120) * 120
    else:
        order = int(order / 2400) * 2400
    print('kv = {}, kv_size = {}, table_id = {}, order = {}'.format(kv, kv_size, table_id, order))
    return np.random.randint(1, 100)


if __name__ == "__main__":
    n = int(arg[1])
    ratios = []
    s = 0
    for i in range(n):
        ratios.append(sample_type())
    s = sum(ratios)
    for i in range(n - 1):
        ratios[i] = int(ratios[i] / s * 100)
    ratios[n-1] = 100 - sum(ratios[:n-1])
    print('ratios = ', ratios)
