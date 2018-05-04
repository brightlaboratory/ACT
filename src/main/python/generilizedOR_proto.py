import numpy as np
import random, math


def add_data_doa_segment(dataset, col, sample, low, high, seed):
    np.random.seed(seed)
    #print(low, high, sample)
    #seg = np.random.random_integers(low, high, sample)
    seg = random.sample(range(low, high+1), sample)
    seg = seg[:-1] if len(seg) > sample else seg
    for i in seg:
        #print(i, col, dataset[i,col])
        dataset[i, col] = 0


n = 10000
dataset = np.ones((10000, 2))
dataset[:, 0] = [i for i in range(1, n+1)]
#doa_rate = [round(0.05*i, 2) for i in range(1, 11)]
doa_rate = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99]
doa_seed = [i*10 for i in range(1, 11)]
block = 1000
partition = int(n/block)
low, high, j = 0, 0, 0
for i in range(1, partition+1):
    low = (i-1)*1000
    high = (i*1000) - 1
    doa_dead = doa_rate[j]*block
    #print(n, 1, doa_dead, low, high, doa_seed[j])
    add_data_doa_segment(dataset, 1, int(doa_dead), low, high, doa_seed[j])
    j += 1


np.savetxt('sample.csv', dataset, delimiter=",")
oddsratio = [1]*9
total_alive = sum(dataset[:, 1])
total_dead = n - total_alive
j = 0
print("Threshold alive_normal dead_abn dead_normal alive_abn ")
for i in range(block, n, block):
    alive_normal = sum(dataset[:i, 1])
    alive_abn = total_alive - alive_normal
    dead_normal = i - alive_normal
    dead_abn = total_dead - dead_normal
    print(i, alive_normal,dead_abn, dead_normal, alive_abn)
    oddsratio[j] = round((alive_normal*dead_abn)/(dead_normal*alive_abn), 2)
    j += 1
print("Oddsratio: ", oddsratio)

