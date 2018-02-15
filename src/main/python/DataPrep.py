import random, math
import numpy as np


class DataPrep:
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns
        self.table = np.array([-1] * (rows * columns)).reshape(rows, columns)
        self.table[:, 0] = [i for i in range(1, rows + 1)]

    def available_ids(self, col, val):
        row_ids = np.where(self.table[:, col] == val)
        row_ids = [i for i in row_ids[0]]
        return row_ids

    def generate_random(self, available_ids, sample, flag=0):
        if flag == 1:
            ref_doa_ids = self.available_ids(6, 0)
            available_ids = list(set(available_ids) - set(ref_doa_ids))
        print(len(available_ids), sample)
        new_ids = [available_ids[index] for index in random.sample(range(0, len(available_ids)), sample)]
        return new_ids

    def add_data_to_col(self, col, count, val):
        free_ids = self.available_ids(col, -1)
        ids = self.generate_random(free_ids, count)
        for id in ids:
            self.table[id, col] = val

    def add_age_data(self, col, sigma, mu, sample):
        s = list(np.random.normal(mu, sigma, sample))
        s = [int(round(i)) for i in s]
        ids = [s[i] for i in random.sample(range(0, len(s)), sample)]
        for index, val in enumerate(ids):
            self.table[index, col] = val

    def add_data_wrt_doa(self, col, low_dead, high_dead, low_alive, high_alive):
        # Generate low_dead
        ref_doa_ids = self.available_ids(6, 0)
        new_ids = self.generate_random(ref_doa_ids, low_dead)
        for id in new_ids:
            self.table[id, col] = 0
        ref_doa_ids = list(set(ref_doa_ids) - set(new_ids))

        # generating high_dead
        new_ids = self.generate_random(ref_doa_ids, high_dead)
        for id in new_ids:
            self.table[id, col] = 1

        # generating low_alive
        ref_col_ids = self.available_ids(col, -1)
        new_ids = self.generate_random(ref_col_ids, low_alive, flag=1)
        # new_ids = list(set(new_ids) - set(ref_doa_ids))
        for id in new_ids:
            self.table[id, col] = 0
            if self.table[id, 6] != 1:
                self.table[id, 6] = 1

        # generating high alive
        ref_col_ids = self.available_ids(col, -1)
        new_ids = self.generate_random(ref_col_ids, high_alive, flag=1)
        for id in new_ids:
            self.table[id, col] = 1
            if self.table[id, 6] != 1:
                self.table[id, 6] = 1


if __name__ == "__main__":
    n = 14397
    mu = 36
    sigma = 19
    n_male = 10921
    n_female = 3476

    n_ptt = 10453
    n_ptt_high = 9627
    n_ptt_low = 826
    n_ptt_dead_low = 329
    n_ptt_dead_high = 752
    n_ptt_alive_low = 497
    n_ptt_alive_high = 8875

    n_platelet = 11290
    n_platelet_high = 10909
    n_platelet_low = 381
    n_plat_dead_low = 116
    n_plat_dead_high = 904
    n_plat_alive_low = 265
    n_plat_alive_high = 10005

    n_pt = 10790
    n_pt_high = 7796
    n_pt_low = 2994
    n_pt_dead_low = 579
    n_pt_dead_high = 489
    n_pt_alive_low = 2415
    n_pt_alive_high = 7307

    n_dead = 1276

    atc = DataPrep(n, 7)

    atc.add_data_to_col(6, n_dead, 0)
    atc.add_data_to_col(2, n_male, 1)
    atc.add_data_to_col(2, n_female, 0)
    atc.add_age_data(col=1, mu=mu, sigma=sigma, sample=n)

    atc.add_data_wrt_doa(3, n_pt_dead_low, n_pt_dead_high, n_pt_alive_low, n_pt_alive_high)
    atc.add_data_wrt_doa(4, n_ptt_dead_low, n_ptt_dead_high, n_ptt_alive_low, n_ptt_alive_high)
    atc.add_data_wrt_doa(5, n_plat_dead_low, n_plat_dead_high, n_plat_alive_low, n_plat_alive_high)
    np.savetxt("table_2_14_v2.csv", atc.table, delimiter=",")
