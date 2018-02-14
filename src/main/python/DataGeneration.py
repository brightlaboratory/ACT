import random, math
import numpy as np

n = 14397
n_male = 10921
n_female = 3476
n_ptt = 10453
n_ptt_high = 9627
n_ptt_low = 826
n_platelet = 11290
n_platelet_high = 10909
n_platelet_low = 381
n_pt = 10790
n_pt_high = 7796
n_pt_low = 2994



patient_id = [i for i in range(0, n+1)]
table = np.array(patient_id)
temp = np.array([-1]*(n+1))
table = np.vstack((table, temp))
table = np.vstack((table, temp))
table = np.vstack((table, temp))
table = np.vstack((table, temp))
table = table.T

print(table.shape)

#Generating data for gender
gender = random.sample(range(1, n+1), n_male)
for i in gender:
    table[i, 1] = 1
sum = 0
for i in range(0, n+1):
    if table[i,1] ==1:
        sum += table[i][1]
    else:
        table[i,1] = 0
print("Count of male: ", sum)

# generating data for pt
pt = random.sample(range(1, n+1), n_pt)
print("len of pt: ", len(pt))
rand_select = random.sample(range(0, len(pt)), n_pt_low)
print("len of rand_select: ", len(rand_select))
sum=0
for i in pt:
    table[i, 2] = 1
    sum += 1
print("Count of pt class: ", sum)
sum=0
for i in range(0, len(pt)):
    if i in rand_select:
        table[pt[i], 2] = 0
        sum += 1
print("Count of pt low class: ", sum)
sum = 0
for i in range(0, n+1):
    if table[i, 2] ==1:
        sum += table[i][2]
print("Count of pt High class: ", sum, "\n\n")


# generating data for ptt
ptt = random.sample(range(1, n+1), n_ptt)
print("len of ptt: ", len(ptt))
rand_select = random.sample(range(0, len(ptt)), n_ptt_low)
print("len of rand_select: ", len(rand_select))
sum=0
for i in ptt:
    table[i, 3] = 1
    sum += 1
print("Count of ptt class: ", sum)
sum=0
for i in range(0, len(ptt)):
    if i in rand_select:
        table[ptt[i], 3] = 0
        sum += 1
print("Count of ptt low class: ", sum)
sum = 0
for i in range(0, n+1):
    if table[i, 3] ==1:
        sum += table[i][3]
print("Count of ptt High class: ", sum, "\n\n")


# generating data for platelets
platelets = random.sample(range(1, n+1), n_platelet)
print("len of platelets: ", len(platelets))
rand_select = random.sample(range(0, len(platelets)), n_platelet_low)
print("len of rand_select: ", len(rand_select))
sum=0
for i in platelets:
    table[i, 4] = 1
    sum += 1
print("Count of platelets class: ", sum)
sum=0
for i in range(0, len(platelets)):
    if i in rand_select:
        table[platelets[i], 4] = 0
        sum += 1
print("Count of platelets low class: ", sum)
sum = 0
for i in range(0, n+1):
    if table[i, 4] ==1:
        sum += table[i][4]
print("Count of platelets High class: ", sum)

np.savetxt("table.csv", table, delimiter=",")


# For age but has few negative values
"""
mu, sigma = 36, 19
s = list(np.random.normal(mu, sigma, n))
s = [int(round(i)) for i in s]
print (s)
"""
