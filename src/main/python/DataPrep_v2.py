import random, math, datetime
import numpy as np
import subprocess as sp


class DataProperties:

    def __init__(self, cohort, gender_odds_ratio, pt_odds_ratio, ptt_odds_ratio, platelet_odds_ratio, hf_odds_ratio, doa_rate=0.15,
                 male_ratio=0.75, pt_abnormal=0.27, ptt_abnormal=0.08, plat_abnormal=0.04, hf_abnormal=0.18):

        self.gender_odds_ratio = gender_odds_ratio
        self.pt_odds_ratio = pt_odds_ratio
        self.ptt_odds_ratio = ptt_odds_ratio
        self.platelet_odds_ratio = platelet_odds_ratio
        self.hf_odds_ratio = hf_odds_ratio
        self.doa_rate = doa_rate
        self.male = math.floor(cohort*male_ratio)
        self.female = cohort - self.male
        self.dead = math.ceil(cohort * doa_rate)
        self.gender_dead = self.dead
        self.pt_dead = self.dead
        self.ptt_dead = self.dead
        self.platelet_dead = self.dead
        self.hf_dead = self.dead
        # self.pt_dead = math.floor(cohort * 0.099)
        # self.ptt_dead = math.floor(cohort*0.103)
        # self.platelet_dead = math.floor(cohort*0.09)
        self.gender_alive = cohort - self.gender_dead
        self.pt_alive = cohort - self.pt_dead
        self.ptt_alive = cohort - self.ptt_dead
        self.platelet_alive = cohort - self.platelet_dead
        self.hf_alive = cohort - self.hf_dead
        self.pt_low = math.ceil(cohort * pt_abnormal)
        self.ptt_low = math.ceil(cohort * ptt_abnormal)
        self.platelet_low = math.ceil(cohort * plat_abnormal)
        self.hf_low = math.ceil(cohort * hf_abnormal)
        self.pt_high = cohort - self.pt_low
        self.ptt_high = cohort - self.ptt_low
        self.platelet_high = cohort - self.platelet_low
        self.hf_high = cohort - self.hf_low

    @staticmethod
    def f(a, b, c):
        if b ** 2 - 4 * a * c < 0:
            # print(b ** 2 - 4 * a * c)
            exit("Complex root detected: ")
        root1 = (-b + math.sqrt(b ** 2 - 4 * a * c)) / (2 * a)
        root2 = (-b - math.sqrt(b ** 2 - 4 * a * c)) / (2 * a)
        if root1 > 0 and root2 > 0:
            s = math.ceil(min(root1, root2))
        elif root1 <= 0:
            s = root2
        else:
            s = root1

        return s

    def get_gender_values(self):
        p = self.gender_odds_ratio - 1
        q = self.female - self.gender_alive - self.gender_odds_ratio * self.female - self.gender_odds_ratio * self.gender_dead
        r = self.gender_odds_ratio * self.female * self.gender_dead

        gender_dead_female = DataProperties.f(p, q, r)
        gender_dead_male = self.gender_dead - gender_dead_female
        gender_alive_male = self.male - gender_dead_male
        gender_alive_female = self.female - gender_dead_female

        return gender_dead_female, gender_dead_male, gender_alive_female, gender_alive_male

    def get_pt_values(self):
        p = self.pt_odds_ratio - 1
        q = self.pt_low - self.pt_alive - self.pt_odds_ratio * self.pt_low - self.pt_odds_ratio * self.pt_dead
        r = self.pt_odds_ratio * self.pt_low * self.pt_dead

        pt_dead_low = DataProperties.f(p, q, r)
        pt_dead_high = self.pt_dead - pt_dead_low
        pt_alive_high = self.pt_high - pt_dead_high
        pt_alive_low = self.pt_low - pt_dead_low

        return pt_dead_low, pt_dead_high, pt_alive_low, pt_alive_high

    def get_ptt_values(self):
        p = self.ptt_odds_ratio - 1
        q = self.ptt_low - self.ptt_alive - self.ptt_odds_ratio * self.ptt_low - self.ptt_odds_ratio * self.ptt_dead
        r = self.ptt_odds_ratio * self.ptt_low * self.ptt_dead

        ptt_dead_low = DataProperties.f(p, q, r)
        ptt_dead_high = self.ptt_dead - ptt_dead_low
        ptt_alive_high = self.ptt_high - ptt_dead_high
        ptt_alive_low = self.ptt_low - ptt_dead_low

        return ptt_dead_low, ptt_dead_high, ptt_alive_low, ptt_alive_high

    def get_platelet_values(self):
        p = self.platelet_odds_ratio - 1
        q = self.platelet_low - self.platelet_alive - self.platelet_odds_ratio * self.platelet_low - self.platelet_odds_ratio * self.platelet_dead
        r = self.platelet_odds_ratio * self.platelet_low * self.platelet_dead

        platelet_dead_low = DataProperties.f(p, q, r)
        platelet_dead_high = self.platelet_dead - platelet_dead_low
        platelet_alive_high = self.platelet_high - platelet_dead_high
        platelet_alive_low = self.platelet_low - platelet_dead_low

        return platelet_dead_low, platelet_dead_high, platelet_alive_low, platelet_alive_high

    def get_hf_values(self):
        p = self.hf_odds_ratio - 1
        q = self.hf_low - self.hf_alive - self.hf_odds_ratio * self.hf_low - self.hf_odds_ratio * self.hf_dead
        r = self.hf_odds_ratio * self.hf_low * self.hf_dead

        hf_dead_low = DataProperties.f(p, q, r)
        hf_dead_high = self.hf_dead - hf_dead_low
        hf_alive_high = self.hf_high - hf_dead_high
        hf_alive_low = self.hf_low - hf_dead_low

        return hf_dead_low, hf_dead_high, hf_alive_low, hf_alive_high



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
            ref_doa_ids = self.available_ids(7, 0)
            available_ids = list(set(available_ids) - set(ref_doa_ids))
        print(len(available_ids), sample)
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
        ref_doa_ids = self.available_ids(7, 0)
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
            if self.table[id, 7] != 1:
                self.table[id, 7] = 1

        # generating high alive
        ref_col_ids = self.available_ids(col, -1)
        new_ids = self.generate_random(ref_col_ids, high_alive, seed, flag=1)
        for id in new_ids:
            self.table[id, col] = 1
            if self.table[id, 7] != 1:
                self.table[id, 7] = 1

def move_data():
    sp.run(["./home/nms/PycharmProjects/ATC/MoveData.sh"])

if __name__ == "__main__":
    n = 10000
    no_of_seed_sets = 10 # used to set the distinct number of seeds sets to be used. Default to 1
    doa = 0.2
    gender_OR = 6
    gender_ratio = 0.75
    pt_OR = 3.3
    pt_abn = 0.28
    ptt_OR = 7.8
    ptt_abn = 0.2
    plat_OR = 4.84
    plat_abn = 0.1
    hf_OR = 3.3
    hf_abn = 0.18
    seed_list = {'age': [], 'gender': [], 'pt': [], 'ptt': [], 'plat': [],'hf': [], 'doa': []}
    j = 1
    for _ in range(no_of_seed_sets):
        seed_list['age'].append(j)
        seed_list['gender'].append(j+1)
        seed_list['pt'].append(j+2)
        seed_list['ptt'].append(j+3)
        seed_list['plat'].append(j+4)
        seed_list['hf'].append(j+5)
        seed_list['doa'].append(j+6)
        j = j+1

    dp_obj = DataProperties(n, gender_OR, pt_OR, ptt_OR, plat_OR, hf_OR, doa, gender_ratio, pt_abn, ptt_abn, plat_abn, hf_abn)
    dt1 = str('{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now()))
    x=0
    for seed in range(no_of_seed_sets):
        print("round: ", seed+1)
        table = DataPrep(n, 8)
        table.add_data_to_col(7, dp_obj.dead, 0, seed_list['doa'][seed])
        # table.add_data_to_col(2, dp_obj.male, 1, seed_list['gender'][seed])
        # table.add_data_to_col(2, dp_obj.female, 0, seed_list['gender'][seed])
        table.add_age_data(col=1, sigma=19, mu=36, sample=n, seed=seed_list['age'][seed])

        n_gen_dead_f, n_gen_dead_m, n_gen_alive_f, n_gen_alive_m = dp_obj.get_gender_values()
        n_pt_dead_low, n_pt_dead_high, n_pt_alive_low, n_pt_alive_high = dp_obj.get_pt_values()
        n_ptt_dead_low, n_ptt_dead_high, n_ptt_alive_low, n_ptt_alive_high = dp_obj.get_ptt_values()
        n_plat_dead_low, n_plat_dead_high, n_plat_alive_low, n_plat_alive_high = dp_obj.get_platelet_values()
        n_hf_dead_low, n_hf_dead_high, n_hf_alive_low, n_hf_alive_high = dp_obj.get_hf_values()

        dt = str('{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now()))
        '''
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
        '''
        table.add_data_wrt_doa(2, n_gen_dead_f, n_gen_dead_m, n_gen_alive_f, n_gen_alive_m, seed_list['gender'][seed])
        table.add_data_wrt_doa(3, n_pt_dead_low, n_pt_dead_high, n_pt_alive_low, n_pt_alive_high, seed_list['pt'][seed])
        table.add_data_wrt_doa(4, n_ptt_dead_low, n_ptt_dead_high, n_ptt_alive_low, n_ptt_alive_high,
                               seed_list['ptt'][seed])
        table.add_data_wrt_doa(5, n_plat_dead_low, n_plat_dead_high, n_plat_alive_low, n_plat_alive_high,
                               seed_list['plat'][seed])
        table.add_data_wrt_doa(6, n_hf_dead_low, n_hf_dead_high, n_hf_alive_low, n_hf_alive_high, seed_list['hf'][seed])

        filename = '/home/nms/PycharmProjects/ATC/data/dataset_{0}{1}{2}{3}{4}{5}{6}_{7}_{8}_{9}_{10}_{11}.csv'.format(
            str(seed_list['age'][seed]), str(seed_list['gender'][seed]), str(seed_list['pt'][seed]),
            str(seed_list['ptt'][seed]), str(seed_list['plat'][seed]), str(seed_list['hf'][seed]),
            str(seed_list['doa'][seed]), str(gender_OR), str(pt_OR), str(ptt_OR), str(plat_OR), dt)

        np.savetxt(filename, table.table, delimiter=",")

    #move_data()
