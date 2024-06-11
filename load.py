import numpy as np

class Load():
    def __init__(self):
        self.max = 0
        self.step = 0
        self.quarter_seg = 0
        self.half_seg = 0
        self.three_quarter_seg = 0
        self.upper_quartil_time_in_highest_segment = 0
        self.highest_segment_durations = []
        self.highest_segment_tracker_list = []
        self.in_highest_seg = False
        self.discharge_power = None
        self.in_above_max_segment = False
        self.corrected_max = 0
        self.clustering_labels = None
        self.clusters = None
        self.min_boundaries_clusters = None
        self.max_boundaries_clusters = None
        self.current_cluster = None
        self.cluster_durations = None

    def update_max(self, new_point):
        if new_point > self.max:
            self.max = new_point

    def update_corrected_max(self, battery_power, new_point):
        if new_point + battery_power > self.corrected_max:
            self.corrected_max = new_point + battery_power

    def update_segments(self):
        self.quarter_seg = self.max/4
        self.half_seg = self.max/2
        self.three_quarter_seg = 3*self.max/4

    def track_high_seg(self, new_point):
        if self.in_highest_seg == False and new_point >= self.three_quarter_seg:
            self.in_highest_seg = True
            self.highest_segment_tracker_list = [new_point]
            #print("Peak Started!")
        elif self.in_highest_seg == True and new_point >= self.three_quarter_seg:
            self.highest_segment_tracker_list.append(new_point)
        elif self.in_highest_seg == True and new_point < self.three_quarter_seg:
            self.in_highest_seg = False
            #print("Peak Ended!")
            duration_in_highest_segment = len(self.highest_segment_tracker_list)
            self.highest_segment_tracker_list = []
            self.highest_segment_durations.append(duration_in_highest_segment)
            self.upper_quartil_time_in_highest_segment = np.quantile(self.highest_segment_durations, 0.9)

    def charge_check(self, new_point):
        if new_point <= self.corrected_max:
            if self.corrected_max <= self.quarter_seg:
                return True, self.quarter_seg - new_point
            else:
                return True, self.corrected_max - new_point
        else:
            return False, 0
        
    def discharge_check(self, battery, new_point):
        if new_point > self.corrected_max and self.in_above_max_segment == False:
            if new_point-self.corrected_max <= battery.capacity/(self.upper_quartil_time_in_highest_segment+1):
                return True, new_point - self.corrected_max
            else:
                self.discharge_power = battery.capacity/(self.upper_quartil_time_in_highest_segment+1)
                self.in_above_max_segment = True
                return True, min(np.round(self.discharge_power), new_point)
        elif new_point > self.corrected_max and self.in_above_max_segment == True:
            return True, min(np.round(self.discharge_power), new_point)
        elif new_point <= self.corrected_max and self.in_above_max_segment == True:
            self.discharge_power = None
            self.in_above_max_segment = False
            return False, 0
        else:
            return False, 0