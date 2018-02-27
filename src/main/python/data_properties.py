import math

class DataProperties:

    def __init__(self, cohort, pt_odds_ratio, ptt_odds_ratio, platelet_odds_ratio, doa_rate= 0.15, seed=1):
        self.cohort = cohort
        self.pt_odds_ratio = pt_odds_ratio
        self.ptt_odds_ratio = ptt_odds_ratio
        self.platelet_odds_ratio = platelet_odds_ratio
        self.doa_rate = doa_rate
        self.seed = seed
        self.dead = math.floor(cohort * doa_rate)
        self.pt_dead = math.floor(cohort * 0.099)
        self.ptt_dead = math.floor(cohort*0.103)
        self.platelet_dead = math.floor(cohort*0.09)
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
            print(b ** 2 - 4 * a * c)
            exit("Complex root detected: ")
        root1 = (-b + math.sqrt(b ** 2 - 4 * a * c)) / (2 * a)
        root2 = (-b - math.sqrt(b ** 2 - 4 * a * c)) / (2 * a)
        return root1, root2

    def get_pt_values(self):
        p = self.pt_odds_ratio - 1
        q = self.pt_low - self.pt_alive - self.pt_odds_ratio*self.pt_low - self.pt_odds_ratio*self.pt_dead
        r = self.pt_odds_ratio*self.pt_low*self.pt_dead
        s1, s2 = DataProperties.f(q, r)
        print(s1, s2)
        s = math.ceil(min(s1, s2))
        pt_dead_low = s
        pt_dead_high = self.pt_dead - pt_dead_low
        pt_alive_high = self.pt_high - pt_dead_high
        pt_alive_low = self.pt_low - s

        return pt_dead_low, pt_alive_low, pt_dead_high, pt_alive_high

    def get_ptt_values(self):
        p = self.ptt_odds_ratio - 1
        q = self.ptt_low - self.ptt_alive - self.ptt_odds_ratio * self.ptt_low - self.ptt_odds_ratio * self.ptt_dead
        r = self.ptt_odds_ratio * self.ptt_low * self.ptt_dead
        s1, s2 = DataProperties.f(q, r)
        print(s1, s2)
        s = math.ceil(min(s1, s2))
        ptt_dead_low = s
        ptt_dead_high = self.ptt_dead - ptt_dead_low
        ptt_alive_high = self.ptt_high - ptt_dead_high
        ptt_alive_low = self.ptt_low - s

        return ptt_dead_low, ptt_alive_low, ptt_dead_high, ptt_alive_high

    def get_platelet_values(self):
        p = self.platelet_odds_ratio - 1
        q = self.platelet_low - self.platelet_alive - self.platelet_odds_ratio * self.platelet_low - self.platelet_odds_ratio * self.platelet_dead
        r = self.platelet_odds_ratio * self.platelet_low * self.platelet_dead
        s1, s2 = DataProperties.f(q, r)
        print(s1, s2)
        print(self.platelet_dead, self.platelet_alive)
        s = math.ceil(min(s1, s2))
        platelet_dead_low = s
        platelet_dead_high = self.platelet_dead - platelet_dead_low
        platelet_alive_high = self.platelet_high - platelet_dead_high
        platelet_alive_low = self.platelet_low - s

        return platelet_dead_low, platelet_alive_low, platelet_dead_high, platelet_alive_high


if __name__=="__main__":
    config1 = DataProperties(11290, 5.5, 8.8, 4.8)
    print(config1.get_pt_values())
    print(config1.get_ptt_values())
    print(config1.get_platelet_values())
