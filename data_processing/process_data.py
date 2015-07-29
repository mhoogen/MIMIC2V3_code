import goslate
import numpy as np
from util_ import util
from util_ import in_out as io
import scipy.stats as sc
import time
import math
import copy

class createProcessedFiles:


    def __init__(self):
        self.patient_dict = {}
        self.headers = []
        self.number_of_days_considered = 6 # 6 days (cf. Caleb)
        self.day_change = 23 # 11pm is the time at which the day changes.
        self.knn_granularity = 15 # 15 minute ranges considered

        # Set the ranges allowed according to the table on page 32/33 of Caleb's thesis.
        self.ranges = {}
        self.ranges['gcs'] = {'min_value': 3, 'max_value': 15}
        self.ranges['weight'] = {'min_value': 20, 'max_value': 300}
        self.ranges['admitwt'] = {'min_value': 20, 'max_value': 300}
        self.ranges['nbpsys'] = {'min_value': 30, 'max_value': 250}
        self.ranges['nbpdias'] = {'min_value': 8, 'max_value': 150}
        self.ranges['nbpmean'] = {'min_value': 20, 'max_value': 250}
        self.ranges['sbp'] = {'min_value': 30, 'max_value': 300}
        self.ranges['dbp'] = {'min_value': 8, 'max_value': 150}
        self.ranges['map'] = {'min_value': 20, 'max_value': 170}
        self.ranges['hr'] = {'min_value': 20, 'max_value': 300}
        self.ranges['resp'] = {'min_value': 20, 'max_value': 300}
        self.ranges['spo2'] = {'min_value': 70, 'max_value': 101}
        self.ranges['cvp'] = {'min_value': -5, 'max_value': 50}
        self.ranges['papmean'] = {'min_value': 0.1, 'max_value': 120}
        self.ranges['papsd'] = {'min_value': 0.1, 'max_value': 120}
        self.ranges['crdindx'] = {'min_value': 0.1, 'max_value': 10}
        self.ranges['svr'] = {'min_value': 0.1, 'max_value': 3200}
        self.ranges['cotd'] = {'min_value': 0.1, 'max_value': 20}
        self.ranges['cofick'] = {'min_value': 0.1, 'max_value': 20}
        self.ranges['pcwp'] = {'min_value': 0.1, 'max_value': 45}
        self.ranges['pvr'] = {'min_value': 0.1, 'max_value': 1000}
        self.ranges['na'] = {'min_value': 115, 'max_value': 160}
        self.ranges['k'] = {'min_value': 1, 'max_value': 10}
        self.ranges['cl'] = {'min_value': 75, 'max_value': 135}
        self.ranges['co2'] = {'min_value': 0.1, 'max_value': 55}
        self.ranges['glucose'] = {'min_value': 0.1, 'max_value': 500}
        self.ranges['bun'] = {'min_value': 0.1, 'max_value': 180}
        self.ranges['creatinine'] = {'min_value': 0.1, 'max_value': 40}
        self.ranges['mg'] = {'min_value': 0.01, 'max_value': 5}
        self.ranges['ast'] = {'min_value': 10, 'max_value': 1000}
        self.ranges['alt'] = {'min_value': 10, 'max_value': 1000}
        self.ranges['ca'] = {'min_value': 4, 'max_value': 14}
        self.ranges['ionca'] = {'min_value': 0, 'max_value': 2.5}
        self.ranges['tbili'] = {'min_value': 0.001, 'max_value': 60}
        self.ranges['dbili'] = {'min_value': 0, 'max_value': 50}
        self.ranges['tprotein'] = {'min_value': 0.01, 'max_value': 15}
        self.ranges['albumin'] = {'min_value': 0.01, 'max_value': 7}
        self.ranges['lactate'] = {'min_value': 0.2, 'max_value': 40}
        self.ranges['troponin'] = {'min_value': 0.01, 'max_value': 100}
        self.ranges['hct'] = {'min_value': 15, 'max_value': 60}
        self.ranges['hgb'] = {'min_value': 4, 'max_value': 20}
        self.ranges['platelets'] = {'min_value': 0.1, 'max_value': 1200}
        self.ranges['inr'] = {'min_value': 0.01, 'max_value': 12}
        self.ranges['pt'] = {'min_value': 0.01, 'max_value': 36}
        self.ranges['ptt'] = {'min_value': 10, 'max_value': 151}
        self.ranges['wbc'] = {'min_value': 0.01, 'max_value': 70}
        self.ranges['rbc'] = {'min_value': 1, 'max_value': 7}
        self.ranges['temp'] = {'min_value': 80, 'max_value': 110}
        self.ranges['artbe'] = {'min_value': -40, 'max_value': 30}
        self.ranges['artco2'] = {'min_value': 1, 'max_value': 60}
        self.ranges['artpaco2'] = {'min_value': 5, 'max_value': 100}
        self.ranges['artpao2'] = {'min_value': 0.1, 'max_value': 500}
        self.ranges['artph'] = {'min_value': 6.5, 'max_value': 8.5}
        self.ranges['fio2set'] = {'min_value': 0.1, 'max_value': 1}
        self.ranges['peepset'] = {'min_value': 0, 'max_value': 50}
        self.ranges['resptot'] = {'min_value': 0.1, 'max_value': 50}
        self.ranges['respset'] = {'min_value': 0.1, 'max_value': 40}
        self.ranges['respson'] = {'min_value': 0.001, 'max_value': 40}
        self.ranges['pip'] = {'min_value': 5, 'max_value': 60}
        self.ranges['plateaupres'] = {'min_value': 5, 'max_value': 60}
        self.ranges['tidvolobs'] = {'min_value': 100, 'max_value': 1100}
        self.ranges['tidvolset'] = {'min_value': 50, 'max_value': 1001}
        self.ranges['tidvolspon'] = {'min_value': 0.1, 'max_value': 1200}
        self.ranges['sao2'] = {'min_value': 80, 'max_value': 101}

        # The numerical features present in the current dataset.
        self.numerical_features = ["GCS","WEIGHT","ADMITWT","NBPSYS","NBPDIAS","NBPMEAN",
                                   "NBP","SBP","DBP","MAP","HR","RESP","SPO2","CVP",
                                   "PAPMEAN","PAPSD","CRDINDX","SVR","COTD","COFCK","PCWP",
                                   "PVR","NA","K","CL","CO2","GLUCOSE","BUN","CREATININE",
                                   "MG","AST","ALT","CA","IONCA","TBILI","DBILI","TPROTEIN",
                                   "ALBUMIN","LACTATE","TROPONIN","HCT","HG","PLATELETS","INR",
                                   "PT","PTT","WBC","RBC","TEMP","ARTBE","ARTCO2","ARTPACO2",
                                   "ARTPAO2","ARTPH","FIO2SET","PEEPSET","RESPTOT","RESPSET",
                                   "RESPSPON","PIP","PLATEAUPRES","TIDVOLOBS","TIDVOLSET","TIDVOLSPON",
                                   "SAO2", "hrmhb", "hrmsa", "hrmva", "orientation_ord", "riker_sas_ord",
                                   "vent_mode_ord", "iabp_ord"]
        self.numerical_features = [feat.lower() for feat in self.numerical_features]

        self.medication_features = ["AGGRESTAT","AMICAR","AMINOPHYLLINE","AMIODARONE","AMRINONE","ARGATROBAN",
                                    "ATIVAN","ATRACURIUM","BIVALIRUDIN","CISTRACURIUM",
                                    "DILAUDID","DILTIAZEM","DOBUTAMINE","DOPAMINE","DOXACURIUM","EPINEPHRINE",
                                    "EPINEPHRINEK","ESMOLOL","FENTANYLCONC","FENTANYL","HEPARIN","INSULIN",
                                    "INTEGRELIN","KETAMINE","LABETOLOL","LASIX","LEPIRUDIN","LEVOPHED",
                                    "LEVOPHEDK","LIDOCAINE","MIDAZOLAM","MILTINONE","MORPHINESOLFATE","NARCAN",
                                    "NATRECOR","NEOSYNEPHRINE","NEOSYNEPHRINEK","NICARDIPINE","NITROGLYCERINE",
                                    "NITROGLYCERINEK","NITROPRUSSIDE","PANCURONIUM","PENTOBARBITOL","PRECEDEX",
                                    "PROCAINAMIDE","PROPOFOL","REOPRO","SANDOSTATIN","TPA","VASOPRESSIN","VECURONIUM"]

        self.medication_features = [feat.lower() for feat in self.medication_features]

        # The categorial mapping (cf. Table 3.2)
        self.categorial_mapping = {}
        self.categorial_mapping['heart_rhythm'] = {'hrmhb':{'1st deg av block':1, '2nd avb mobitz 2':2, '2nd avb/mobitz i':3,
                                                           'wenckbach':4, 'comp heart block':5, 'default':0},
                                                  'hrmpaced': {'paced':1, 'a paced':1, 'av paced':1, 'v paced':1,
                                                               'zoll paced':1, 'default':0},
                                                  'hrmsa':{'parox atr tachy':1, 'sinus arrhythmia':2, 'supravent tachy':3,
                                                           'wand.atrial pace':4, 'multifocalatrtrach':5, 'atrial fib':6,
                                                           'atrial flutter':7, 'default':0},
                                                  'hrmva': {'junctional':1, 'idioventricular':2, 'vent. tachy':3,
                                                            'ventricular fib':4, 'asystole':5, 'default':0}}
        self.categorial_mapping['ectopy_type'] = {'pvc_bin':{'pvc\'s':1, 'v quadrigeminy':1, 'vent. trigeminy':1,
                                                           'vent. bigeminy':1, 'default':0},
                                                  'pac_bin': {'pac s':1, 'a quadrigeminy':1, 'atrial trigeminy':1, 'atrial bigeminy':1,
                                                              'default':0},
                                                  'pnc_bin':{'pnc s':1, 'n quadrigeminy':1, 'nodal trigeminy':1, 'default':0}}
        self.categorial_mapping['ectopy_freq'] = {'ect_freq_bin':{'rare':1, 'occasional':1, 'frequent':1,
                                                           'runs vtach':1, 'default':0}}
        self.categorial_mapping['code_status'] = {'dni_bin':{'do not intubate':1, 'default':0},
                                                  'no_cpr_bin':{'cpr not indicate':1, 'default':0},
                                                  'dnr_bin':{'do not resuscita':1, 'default':0},
                                                  'comfort_meas_bin':{'comfort measures':1, 'default':0},
                                                  'other_code_bin':{'other/remarks':1, 'default':0},
                                                  'full_code_bin':{'full code':1, 'default':0}}
        self.categorial_mapping['fallrisk'] = {'fall_risk_bin':{'yes':1, 'default':0}}
        self.categorial_mapping['orientation'] = {'orientation_ord':{'oriented x 3':1, 'oriented x 2':2, 'oriented x 1':3,
                                                                     'disoriented':4, 'default':0},
                                                  'orient_unable_ass_bin':{'unable to assess':1, 'default':0}}
        self.categorial_mapping['rikersas'] = {'riker_sas_ord':{'unarousable':1, 'very sedated':2, 'sedated':3,
                                                                     'calm/cooperative':4, 'agitated':5,
                                                                     'very agitated':6, 'danger agitation':6, 'default':0}}
        self.categorial_mapping['vent'] = {'vent_bin':{'7200a':1, 'drager':1, 'other/remarks':1,
                                                                     'servo 900c':1, 'default':0}}
        self.categorial_mapping['ventmode'] = {'vent_mode_ord':{'assist control':1, 'cmv':2, 'cpap':3, 'cpap+ps':4,
                                                                     'pressure control':5, 'pressure support':6, 'simv':7,
                                                                     'simv+ps':8, 'tcpcv':9, 'other/remarks':10, 'default':0}}
        self.categorial_mapping['pacemaker'] = {'pacemaker_bin':{'epicardial wires':1, 'permanent':1, 'transcutaneous':1,
                                                                     'transvenous':1, 'default':0}}
        self.categorial_mapping['trach'] = {'trach_bin':{'#4':1, '#5':1, '#6':1,'#7':1, '#8':1, '#9':1,
                                                                     '10':1, 'other/remarks':1, 'default':0}}
        self.categorial_mapping['skincolor'] = {'pale_skin_bin':{'pale':1, 'ashen':1, 'dusky':1,'cyanotic':1, 'default':0},
                                                 'flush_skin_bin':{'flushed':1, 'mottled':1, 'default':0},
                                                 'jaundice_skin_bin':{'jaundiced':1, 'default':0}}
        self.categorial_mapping['skinintegrity'] = {'impaired_skin_bin':{'absent':1, 'impaired':1, 'other/remarks':1,'default':0}}
        self.categorial_mapping['iabp_setting'] = {'iabp_bin':{'1:1':1, '1:2':1, '1:3':1, '1:4':1, 'default':0},
                                                   'iabp_ord':{'1:4':1, '1:3':2, '1:2':3, '1:1':4, 'default':0}}
        self.categorial_mapping['servicetype'] = {'svother_bin':{'other':1, 'default':0},
                                                   'svcsicu_bin':{'csicu':1, 'default':0},
                                                   'svnsicu_bin':{'nsicu':1, 'default':0},
                                                   'svmicu_bin':{'micu':1, 'default':0},
                                                   'svmsicu_bin':{'msuci':1, 'default':0},
                                                   'svccu_bin':{'ccu':1, 'default':0},
                                                   'svcsru_bin':{'csru':1, 'default':0}}
        self.categorial_mapping['sex'] = {'sex_bin':{'m':0, 'default':1}}
        self.categorial_mapping['ethnicity_descr'] = {'white':{'white':1, 'default':0}} # Needs to be extended!!

        self.features_to_be_excluded = ["HOSPITAL_EXPIRE_FLG","DAYSFROMDISCHTODEATH","AGEATDEATH","INTIME",
                                        "OUTTIME","LOS","FIRST_CAREUNIT","LAST_CAREUNIT", "CHARTTIME"]
        self.features_to_be_excluded = [feat.lower() for feat in self.features_to_be_excluded]
        self.fixed_features = ['ID', 'label', 'sex_bin', 'white']
        self.fixed_features = [feat.lower() for feat in self.fixed_features]

        self.slope_windows = {}
        self.four_twenty_eight_windows = ["NBPSYS","NBPDIAS","NBPMEAN",
                                   "NBP","SBP","DBP","MAP","HR","RESP","SPO2","CVP",
                                   "PAPMEAN","PAPSD","CRDINDX","SVR","COTD","COFCK","PCWP",
                                   "PVR"]
        self.four_twenty_eight_windows = [feat.lower() for feat in self.four_twenty_eight_windows]

        for attribute in self.four_twenty_eight_windows:
            self.slope_windows[attribute] = [4,28]

        self.twenty_eight_windows = ["GCS","WEIGHT","NA","K","CL","CO2","GLUCOSE","BUN","CREATININE",
                                   "MG","AST","ALT","CA","IONCA","TBILI","DBILI","TPROTEIN",
                                   "ALBUMIN","LACTATE","TROPONIN","HCT","HG","PLATELETS","INR",
                                   "PT","PTT","WBC","RBC","TEMP","ARTBE","ARTCO2","ARTPACO2",
                                   "ARTPAO2","ARTPH","FIO2SET","PEEPSET","RESPTOT","RESPSET",
                                   "RESPSPON","PIP","PLATEAUPRES","TIDVOLOBS","TIDVOLSET","TIDVOLSPON",
                                   "SAO2"]
        self.twenty_eight_windows = [feat.lower() for feat in self.twenty_eight_windows]

        for attribute in self.twenty_eight_windows:
            self.slope_windows[attribute] = [28]

    # This function should implement the derived variables as shown in Table 3.8 of Caleb's thesis as well
    # as Table 3.9
    def derived_variables(self, attributes, patient_measurements):
        return [attributes, patient_measurements]

    def perform_regression(self, times, values):
        new_values = []
        if (len(values)) < 2:
            return 0

        #slope, intercept, r_value, p_value, std_err = sc.linregress(times, values)
        X = np.array(np.column_stack(times))
        X = np.vstack([np.column_stack(times), np.ones(len(times))]).T
        Y = np.array(values)
        W = np.linalg.lstsq(X,Y)[0]
        return W[0]

    # For windows of less than 24 hours.
    def derive_windows(self, window):
        window_number = 1
        windows_dict = {}
        start_hours = self.day_change
        end_hours = self.day_change

        while math.floor(float(end_hours-self.day_change)/24) <= self.number_of_days_considered:
            windows_dict[window_number] = {'start_hour':0, 'end_hour':0}
            windows_dict[window_number]['start_hour'] = start_hours
            end_hours = start_hours + window
            windows_dict[window_number]['end_hour'] = end_hours
            if window > 24:
                start_hours = end_hours - (window-24)
            else:
                start_hours = end_hours
            window_number += 1
        return windows_dict

    def derive_values_for_window(self, attribute, timestamps, values, window, type):
        windows_dict = self.derive_windows(window)
        new_values = []
        new_attributes = []

        if type == 0:
            expected_number_frames = int(math.ceil(float(self.number_of_days_considered * 24) / window))
        elif type == 1:
            expected_number_frames = int(math.ceil(float(24) / window))
        # First transform the time stamps to hours and deduct the initial timestamp.

        if len(timestamps) > 0:
            start_hour = time.localtime(timestamps[0]).tm_hour
            if start_hour < self.day_change:
                start_hour += 24
            hourly_timestamps = [((float(ts)-float(timestamps[0]))/(60*60)) + start_hour for ts in timestamps]
            minute_timestamps = [float(ts)/60 for ts in timestamps]
        else:
            hourly_timestamps = []
            minute_timestamps = []

        for frame in range(1, expected_number_frames+1):
            start_hours = windows_dict[frame]['start_hour']
            end_hours = windows_dict[frame]['end_hour']
            # Get all time points within this range

            indices = [i for i,j in enumerate(hourly_timestamps) if j < end_hours and j >= start_hours]
            new_attributes.append('slope_windows_' + str(frame*window) + '_' + str(attribute))

            if len(indices) > 0:
                new_values.append(self.perform_regression([minute_timestamps[i] for i in indices], [values[i] for i in indices]))
            else:
                new_values.append(0)

        return new_attributes, new_values

    # For this category we want to find the
    def derive_numerical_features(self, attribute, timestamps, values, type):
        # Select the timepoints and values that have a value
        remove_indices = [i for i,val in enumerate(values) if val == '']
        values = [float(i) for j, i in enumerate(values) if j not in remove_indices]
        timestamps = [float(i) for j, i in enumerate(timestamps) if j not in remove_indices]

        new_attributes = []
        new_values = []
        if self.slope_windows.has_key(attribute):
            windows = self.slope_windows[attribute]
        else:
            windows = [28] # 28 is considered to be the default value.

        for window in windows:
            window_attributes, window_values = self.derive_values_for_window(attribute, timestamps, values, window, type)
            new_attributes.extend(window_attributes)
            new_values.extend(window_values)

        # And also calculate the summary statistics:
        new_attributes.append('mean_' + attribute)
        new_attributes.append('max_' + attribute)
        new_attributes.append('min_' + attribute)
        new_attributes.append('std_' + attribute)
        if len(values) > 0:
            new_values.append(np.mean(values))
            new_values.append(max(values))
            new_values.append(min(values))
            new_values.append(np.std(values))
        else:
            new_values.extend([0,0,0,0])


        return new_attributes, new_values

    def determine_type(self, list):
        list_type = 'numeric'
        for element in list:
            if not type(element) is float and not type(element) is int:
                list_type = 'non numeric'
        return list_type

    # The rest category, assumed for now that we simply use the mean value.
    def derive_simple_aggregate(self, attribute, timestamps, values):
        # First get the actual non null values
        final_value = 0
        filtered_values = [v for v in values if v != '']
        if (len(filtered_values) > 0):
            # Determine the type: for numerical we take the average, for
            # categorial we use the most frequently occurring item.
            if self.determine_type(filtered_values) == 'numeric':
                final_value = sum(filtered_values)/len(values)
            else:
                final_value = max(values)
                try:
                    final_value = float(final_value)
                except ValueError:
                    final_value = 0
                    # print attribute
                    # print final_value
        return [attribute], [final_value]


    def aggregate_attribute(self, attribute, timestamps, values, type):
        if attribute == 'subject_id' or attribute == 'label':
            return [attribute], [values[0]]
        elif attribute in self.features_to_be_excluded:
            return [], []
        elif attribute in self.numerical_features:
            return self.derive_numerical_features(attribute, timestamps, values, type)
        else:
            # Binary features assumed for the remainder.
            return self.derive_simple_aggregate(attribute, timestamps, values)

    def aggregate_day_sdas_das(self, attributes, patient_measurements, day_count, type):
        [attributes_extended, patient_measurements] = self.derived_variables(attributes, patient_measurements)

        final_attributes = []
        final_values = []
        timestamps = patient_measurements[:,attributes_extended.index('charttime')].tolist()
        for i in range(0, len(attributes_extended)):
            [attributes_found, values_found] = self.aggregate_attribute(attributes[i], timestamps, patient_measurements[:,i].tolist(), type)
            if len(attributes) > 0:
                final_attributes.extend(attributes_found)
                final_values.extend(values_found)

        final_attributes.insert(1, 'day')
        final_values.insert(1, day_count)
        return final_attributes, final_values

    def to_struct_time(self, timestamps):
        struct_timestamps = []
        for timestamp in timestamps:
            struct_timestamps.append(time.localtime(float(timestamp)))
        return struct_timestamps

    def time_point_present(self, current_hours, current_minutes, timestamps):
        for i in range(0, len(timestamps)):
            if timestamps[i].tm_hour == current_hours and timestamps[i].tm_min == current_minutes:
                return i
        return -1

    def aggregate_day_knn(self, attributes, patient_measurements, day_count):
        # print '======' + str(day_count) + ' ' + str(attributes)
        [attributes_extended, patient_measurements] = self.derived_variables(copy.deepcopy(attributes), patient_measurements)

        timestamps = patient_measurements[:,attributes_extended.index('charttime')].tolist()
        structured_timestamps = self.to_struct_time(timestamps)
        current_minutes = 0

        default_row = [np.nan] * len(attributes_extended)
        for i in range(0, len(attributes_extended)):
            if attributes_extended[i] in self.fixed_features:
                default_row[i] = patient_measurements[0,i]
        default_row.insert(1, day_count) # Add a value for the day
        default_row.insert(2, 0) # Add a value for the current time point
        attributes_extended.insert(1,'day')
        attributes_extended.insert(2,'time')
        processed_measurements = np.zeros((0,len(attributes_extended)))

        for time in range(0, int(((float(60)/self.knn_granularity)*24))):
            index = self.time_point_present((math.floor(float(current_minutes)/60) + self.day_change) % 24, current_minutes % 60, structured_timestamps)
            if index == -1:
                row = default_row
                row[2] = time
            else:
                row = patient_measurements[index,:].tolist()
                row.insert(1, day_count)
                row.insert(2, time)
                new_row = []
                for r in row:
                    try:
                        number = float(r)
                    except:
                        number = np.nan
                    new_row.append(number)
                row = new_row
            processed_measurements = np.append(processed_measurements, np.column_stack(row).astype(float), axis=0)
            current_minutes += self.knn_granularity

        for r in range(0, len(self.features_to_be_excluded)):
            index = attributes_extended.index(self.features_to_be_excluded[r])
            attributes_extended.pop(index)
            processed_measurements = np.delete(processed_measurements, index, axis=1)
        return [attributes_extended, processed_measurements]

    def aggregate_day(self, attributes, patient_measurements, day_count, type):
        if type == 0 or type == 1:
            return self.aggregate_day_sdas_das(attributes, patient_measurements, day_count, type)
        elif type == 2:
            return self.aggregate_day_knn(attributes, patient_measurements, day_count)

    # Aggregation for both the SDAS and DAS approach, if more fine-grained predictions are
    # required this should slightly change.

    def aggregate(self, type):

        # The numpy data structure in which all aggregated data will be stored, the day will replace the time
        # The ID will also be included.
        aggr_set = np.zeros((0, 0))
        new_attributes = []

        for ID in self.patient_dict:
            # we start with day 1 and continue until either
            day_count = 1
            index = 0
            daily_set = np.zeros((0, len(self.headers)+1))
            prev_time_point = self.patient_dict[ID]['charttime'][index]
            extended_headers = ['id']
            extended_headers.extend(self.headers)

            while day_count <= self.number_of_days_considered and index < len(self.patient_dict[ID]['charttime']):
                curr_time_point = self.patient_dict[ID]['charttime'][index]

                if ((prev_time_point.tm_hour < self.day_change and curr_time_point.tm_hour >= self.day_change) or
                     (prev_time_point.tm_mday != curr_time_point.tm_mday and
                      (prev_time_point.tm_hour < self.day_change and curr_time_point.tm_hour < self.day_change))):
                    # We have changed to a new day, aggregate the current dataset and add it to the full set.
                    [attr, aggregated_values] = self.aggregate_day(extended_headers, daily_set, day_count, type)

                     # Set the attributes in case we haven't done so yet.
                    if len(new_attributes) == 0:
                        new_attributes = attr
                        aggr_set = np.zeros((0, len(attr)))
                    if type == 2:
                        for i in range(0, aggregated_values.shape[0]):
                            aggr_set = np.append(aggr_set, np.column_stack(aggregated_values[i,:]), axis=0)
                    else:
                        aggr_set = np.append(aggr_set, np.column_stack(aggregated_values), axis=0)
                    if type == 1:
                        # For the case of the DAS model we start with an empty set again
                        daily_set = np.zeros((0, len(self.headers)+1))
                    day_count += 1
                    if day_count > self.number_of_days_considered:
                        break

                # Look at the current data entry
                current_values = []
                current_values.append(ID)
                # Determine the values of the current case
                for i in range(0, len(self.headers)):
                    if self.headers[i] == 'charttime':
                        current_values.append(time.mktime(self.patient_dict[ID][self.headers[i]][index]))
                    else:
                        current_values.append(self.patient_dict[ID][self.headers[i]][index])
                daily_set = np.append(daily_set, np.column_stack(current_values), axis=0)
                index += 1
                prev_time_point = copy.deepcopy(curr_time_point)

                # If this is the last one, we should stop.
                if index == len(self.patient_dict[ID]['charttime']):
                    [attr, aggregated_values] = self.aggregate_day(extended_headers, daily_set, day_count, type)
                    if len(new_attributes) == 0:
                        new_attributes = attr
                        aggr_set = np.zeros((0, len(attr)))
                    if type == 2:
                        for i in range(0, aggregated_values.shape[0]):
                            aggr_set = np.append(aggr_set, np.column_stack(aggregated_values[i,:]), axis=0)
                    else:
                        aggr_set = np.append(aggr_set, np.column_stack(aggregated_values), axis=0)


        aggr_set = aggr_set.astype(np.float64, copy=False)

        # print aggr_set.shape
        return new_attributes, aggr_set


    # Aggregate the data in a way which depends on the type.
    def aggregate_data(self, type):
        # This allows the aggregation of data into learnable sets, depending on the type this can be
        # per day, etc.

        if type == 0:
            return self.aggregate(type)
        elif type == 1:
            return self.aggregate(type)
        elif type == 2:
            return self.aggregate(type)
        #else:
            # Not implemented yet.....
        return

    # Filter values based on the predifined min and max values.
    def min_max_filter(self, feature, value):

        if self.ranges.has_key(feature) and not value == '':
            if float(value) < self.ranges[feature]['min_value'] or float(value) > self.ranges[feature]['max_value']:
                return ''
        return value

    # Derive extensions for categorial attributes.
    def derive_categorial_extensions(self, feature, value, type):
        values = []
        features = []
        if self.categorial_mapping.has_key(feature):
            for mapping in self.categorial_mapping[feature]:
                features.append(mapping)
                if self.categorial_mapping[feature][mapping].has_key(value.lower()):
                    values.append(self.categorial_mapping[feature][mapping][value.lower()])
                else:
                    if type == 0 or type == 1:
                        values.append(self.categorial_mapping[feature][mapping]['default'])
                    else:
                        if value.lower() == '':
                            values.append(np.nan)
                        else:
                            values.append(self.categorial_mapping[feature][mapping]['default'])
        else:
            features = [feature]
            values = [value]
        return features, values

    # Process attributes that deserve special attention.
    def process_value_individual(self, feature, value, type):
        if feature in self.numerical_features:
            # We should look the min/max ranges now
            return [feature], [self.min_max_filter(feature, value)]
        elif self.categorial_mapping.has_key(feature):
            return self.derive_categorial_extensions(feature, value, type)
        else:
            return [feature], [value]

    # Determine the value of the class, 0 means survived, 1 means died.
    def determine_class(self, daysdischtodeath, expire_flag):
        if daysdischtodeath < 30 or not expire_flag == 'N':
            return 1
        return 0

    # Type defines the type of datasets created:
    # - 0 for SDAS (cf. thesis Caleb, daily summed over days)
    # - 1 for DASn (again following thesis Caleb, daily, days are independent)
    # - 2 for KNN where data is not actually aggregated but just stored in a
    #     time uniform way

    def read_files_and_calculate_attributes(self, file, type=0):

        rows = io.read_csv(file, ';');
        counter = 0
        ids = []
        dataset_headers = []

        for row in rows:
            # Assuming the headers are in the first row.
            if counter == 0:
                temp_dataset_headers = row[1:len(row)]

                # Create all headers, also of derived categorial attributes
                # attributes over time and derivations of multiple attributes combined
                # will be derived later.

                for header in temp_dataset_headers:
                    header = header.lower()
                    if 'hold_' in header:
                        header = header[5:len(header)]
                    if self.categorial_mapping.has_key(header):
                        for var in self.categorial_mapping[header]:
                            self.headers.append(var)
                    else:
                        self.headers.append(header)
                    dataset_headers.append(header)
                self.headers.append('label')
                counter += 1
            else:
                # Assuming ID is the first attribute.
                id = row[0]
                if id not in ids:
                    ids.append(id)
                    self.patient_dict[id] = {}
                    for header in self.headers:
                        self.patient_dict[id][header] = []
                # Get the time to order based upon it
                timestamp = time.strptime(row[self.headers.index('charttime')+1][0:17], "%d-%m-%y %H:%M:%S")
                times = self.patient_dict[id]['charttime']
                # Currently no ordering of the times assumed. If they are, just append at the end
                index = 0
                while index < len(times) and times[index] < timestamp:
                    index += 1
                for row_index in range(1, len(row)):
                    if dataset_headers[row_index-1] == 'charttime':
                        self.patient_dict[id]['charttime'].insert(index, timestamp)
                    else:
                        # Determine the values (there can be multiple in the case of categorial attributes)
                        [features, values] = self.process_value_individual(dataset_headers[row_index-1], row[row_index].replace(',','.'), type)
                        for i in range(0, len(values)):
                            self.patient_dict[id][features[i]].insert(index, values[i])
                # Now assign the label
                self.patient_dict[id]['label'].insert(index, self.determine_class(self.patient_dict[id]['daysfromdischtodeath'][index], self.patient_dict[id]['hospital_expire_flg'][index]))
        return self.aggregate_data(type)