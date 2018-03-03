import random, math, datetime
import numpy as np


class DataProperties:

    def __init__(self, cohort, pt_odds_ratio, ptt_odds_ratio, platelet_odds_ratio, doa_rate=0.15, seed=1):
        self.cohort = cohort
        self.pt_odds_ratio = pt_odds_ratio
        self.ptt_odds_ratio = ptt_odds_ratio
        self.platelet_odds_ratio = platelet_odds_ratio
        self.doa_rate = doa_rate
        self.seed = seed
        self.dead = math.floor(cohort * doa_rate)
        self.male = math.floor(cohort * 0.75)
        self.female = cohort - self.male
        self.pt_dead = self.dead
        self.ptt_dead = self.dead
        self.platelet_dead = self.dead
        # self.pt_dead = math.floor(cohort * 0.099)
        # self.ptt_dead = math.floor(cohort*0.103)
        # self.platelet_dead = math.floor(cohort*0.09)
        self.pt_alive = cohort - self.pt_dead
        self.ptt_alive = cohort - self.ptt_dead
        self.platelet_alive = cohort - self.platelet_dead
        self.pt_high = math.floor(cohort * 0.723)
        self.ptt_high = math.floor(cohort * 0.921)
        self.platelet_high = math.floor(cohort * 0.966)
        self.pt_low = math.floor(cohort * 0.277)
        self.ptt_low = math.floor(cohort * 0.079)
        self.platelet_low = math.floor(cohort * 0.034)

    @staticmethod
    def f(a, b, c):
        if b ** 2 - 4 * a * c < 0:
            # print(b ** 2 - 4 * a * c)
            exit("Complex root detected: ")
        root1 = (-b + math.sqrt(b ** 2 - 4 * a * c)) / (2 * a)
        root2 = (-b - math.sqrt(b ** 2 - 4 * a * c)) / (2 * a)
        return root1, root2

    def get_pt_values(self):
        p = self.pt_odds_ratio - 1
        q = self.pt_low - self.pt_alive - self.pt_odds_ratio * self.pt_low - self.pt_odds_ratio * self.pt_dead
        r = self.pt_odds_ratio * self.pt_low * self.pt_dead
        s1, s2 = DataProperties.f(p, q, r)
        # print(s1, s2)
        s = math.ceil(min(s1, s2))
        pt_dead_low = s
        pt_dead_high = self.pt_dead - pt_dead_low
        pt_alive_high = self.pt_high - pt_dead_high
        pt_alive_low = self.pt_low - s

        return pt_dead_low, pt_dead_high, pt_alive_low, pt_alive_high

    def get_ptt_values(self):
        p = self.ptt_odds_ratio - 1
        q = self.ptt_low - self.ptt_alive - self.ptt_odds_ratio * self.ptt_low - self.ptt_odds_ratio * self.ptt_dead
        r = self.ptt_odds_ratio * self.ptt_low * self.ptt_dead
        s1, s2 = DataProperties.f(p, q, r)
        # print(s1, s2)
        s = math.ceil(min(s1, s2))
        ptt_dead_low = s
        ptt_dead_high = self.ptt_dead - ptt_dead_low
        ptt_alive_high = self.ptt_high - ptt_dead_high
        ptt_alive_low = self.ptt_low - s

        return ptt_dead_low, ptt_dead_high, ptt_alive_low, ptt_alive_high

    def get_platelet_values(self):
        p = self.platelet_odds_ratio - 1
        q = self.platelet_low - self.platelet_alive - self.platelet_odds_ratio * self.platelet_low - self.platelet_odds_ratio * self.platelet_dead
        r = self.platelet_odds_ratio * self.platelet_low * self.platelet_dead
        s1, s2 = DataProperties.f(p, q, r)
        # print(s1, s2)
        # print(self.platelet_dead, self.platelet_alive)
        s = math.ceil(min(s1, s2))
        platelet_dead_low = s
        platelet_dead_high = self.platelet_dead - platelet_dead_low
        platelet_alive_high = self.platelet_high - platelet_dead_high
        platelet_alive_low = self.platelet_low - s

        return platelet_dead_low, platelet_dead_high, platelet_alive_low, platelet_alive_high


class DataPrep:
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns
        # self.seed = seed
        self.table = np.array([-1] * (rows * columns)).reshape(rows, columns)
        self.table[:, 0] = [i for i in range(1, rows + 1)]

    def available_ids(self, col, val):
        row_ids = np.where(self.table[:, col] == val)
        row_ids = [i for i in row_ids[0]]
        return row_ids

    def generate_random(self, available_ids, sample, seed=1, flag=0):
        if flag == 1:
            ref_doa_ids = self.available_ids(6, 0)
            available_ids = list(set(available_ids) - set(ref_doa_ids))
        # print(len(available_ids), sample)
        random.seed(seed)
        new_ids = [available_ids[index] for index in random.sample(range(0, len(available_ids)), sample)]
        return new_ids

    def add_data_to_col(self, col, count, val, seed=1):
        free_ids = self.available_ids(col, -1)
        ids = self.generate_random(free_ids, count, seed)
        for id in ids:
            self.table[id, col] = val

    def add_age_data(self, col, sigma, mu, sample, seed=1):
        random.seed(seed)
        s = list(np.random.normal(mu, sigma, sample))
        s = [int(round(i)) for i in s]
        random.seed(seed)
        ids = [s[i] for i in random.sample(range(0, len(s)), sample)]
        for index, val in enumerate(ids):
            self.table[index, col] = abs(val)

    def add_data_wrt_doa(self, col, low_dead, high_dead, low_alive, high_alive, seed=1):
        # Generate low_dead
        ref_doa_ids = self.available_ids(6, 0)
        new_ids = self.generate_random(ref_doa_ids, low_dead, seed)
        for id in new_ids:
            self.table[id, col] = 0
        ref_doa_ids = list(set(ref_doa_ids) - set(new_ids))

        # generating high_dead
        new_ids = self.generate_random(ref_doa_ids, high_dead, seed)
        for id in new_ids:
            self.table[id, col] = 1

        # generating low_alive
        ref_col_ids = self.available_ids(col, -1)
        new_ids = self.generate_random(ref_col_ids, low_alive, seed, flag=1)
        # new_ids = list(set(new_ids) - set(ref_doa_ids))
        for id in new_ids:
            self.table[id, col] = 0
            if self.table[id, 6] != 1:
                self.table[id, 6] = 1

        # generating high alive
        ref_col_ids = self.available_ids(col, -1)
        new_ids = self.generate_random(ref_col_ids, high_alive, seed, flag=1)
        for id in new_ids:
            self.table[id, col] = 1
            if self.table[id, 6] != 1:
                self.table[id, 6] = 1

    def load_table(self):

        pass


if __name__ == "__main__":
    n = 10000
    doa_list = [0.15, 0.20, 0.25, 0.30]
    ptr_list = [5.0, 5.5, 6.0, 6.5]
    pttr_list = [7.0, 7.5, 8.0, 8.5]
    plat_list = [4.0, 4.5, 5.0, 5.5]
    seed_list = {'age': 1, 'gender': 2, 'pt': 3, 'ptt': 4, 'plat': 5, 'doa': 6}

    n_iter_ = len(doa_list) * len(ptr_list) * len(pttr_list) * len(plat_list)
    print(n_iter_)
    dt1 = str('{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now()))
    for i in doa_list:
        for j in ptr_list:
            for k in pttr_list:
                for l in plat_list:
                    dp_obj = DataProperties(n, doa_rate=i, pt_odds_ratio=j, ptt_odds_ratio=k, platelet_odds_ratio=l)
                    table = DataPrep(n, 7)
                    table.add_data_to_col(6, dp_obj.dead, 0, seed_list['doa'])
                    table.add_data_to_col(2, dp_obj.male, 1, seed_list['gender'])
                    table.add_data_to_col(2, dp_obj.female, 0, seed_list['gender'])
                    table.add_age_data(col=1, sigma=19, mu=36, sample=n, seed=seed_list['age'])
                    n_pt_dead_low, n_pt_dead_high, n_pt_alive_low, n_pt_alive_high = dp_obj.get_pt_values()
                    n_ptt_dead_low, n_ptt_dead_high, n_ptt_alive_low, n_ptt_alive_high = dp_obj.get_ptt_values()
                    n_plat_dead_low, n_plat_dead_high, n_plat_alive_low, n_plat_alive_high = dp_obj.get_platelet_values()

                    dt = str('{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now()))
                    filename = '/home/nms/PycharmProjects/ATC/data/' + "oddsratio_src_" + str(i) + "_" + str(j) + "_" \
                               + str(k) + "_" + str(l) + "_" + dt + ".csv"
                    d = {'pt': [n_pt_dead_low, n_pt_dead_high, n_pt_alive_low, n_pt_alive_high],
                         'ptt': [n_ptt_dead_low, n_ptt_dead_high, n_ptt_alive_low, n_ptt_alive_high],
                         'plat': [n_plat_dead_low, n_plat_dead_high, n_plat_alive_low, n_plat_alive_high],
                         'pt_or': (n_pt_dead_low*n_pt_alive_high)/(n_pt_dead_high*n_pt_alive_low),
                         'ptt_or': (n_ptt_dead_low * n_ptt_alive_high) / (n_ptt_dead_high * n_ptt_alive_low),
                         'plat_or': (n_plat_dead_low * n_plat_alive_high) / (n_plat_dead_high * n_plat_alive_low)}
                    with open('/home/nms/PycharmProjects/ATC/data/oddsratio_src_' + dt1 + '.txt', 'a') as tf:
                        tf.write(filename)
                        tf.write(str(d))
                        # tf.write(str(d['ptt']))
                        # tf.write(str(d['plat']))

                    table.add_data_wrt_doa(3, n_pt_dead_low, n_pt_dead_high, n_pt_alive_low, n_pt_alive_high)
                    table.add_data_wrt_doa(4, n_ptt_dead_low, n_ptt_dead_high, n_ptt_alive_low, n_ptt_alive_high)
                    table.add_data_wrt_doa(5, n_plat_dead_low, n_plat_dead_high, n_plat_alive_low, n_plat_alive_high)

                    filename = '/home/nms/PycharmProjects/ATC/data/dataset_{0}{1}{2}{3}{4}{5}_{6}_{7}_{8}_{9}_{10}.csv'.format(
                        str(seed_list['age']), str(seed_list['gender']), str(seed_list['pt']), str(seed_list['ptt']),
                        str(seed_list['plat']), str(seed_list['doa']), str(i), str(j), str(k), str(l), dt)

                    np.savetxt(filename, table.table, delimiter=",")

    tf.close()
