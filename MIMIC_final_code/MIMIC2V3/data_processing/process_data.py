import numpy as np
from util_ import util
from util_ import in_out as io
import scipy.stats as sc
import time
import math
import copy
import datetime

class createProcessedFiles:


    def __init__(self):
        self.writer = ''
        self.patient_dict = {}     #Set (no duplicate values)
        self.headers = []          #list
        self.number_of_days_considered = 1 # First 1 day, should be 6 days (cf. Caleb)
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
        # "AMICAR", removed by Ali (not availabe in the the dataset)
        self.medication_features = ["AGGRESTAT","AMINOPHYLLINE","AMIODARONE","AMRINONE","ARGATROBAN",
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

        self.features_to_be_excluded = ["ICUSTAY_EXPIRE_FLG","DAYSFROMDISCHTODEATH","AGEATDEATH","ICUSTAY_INTIME",
                                        "ICUSTAY_OUTTIME","ICUSTAY_LOS","ICUSTAY_FIRST_CAREUNIT","ICUSTAY_LAST_CAREUNIT", "CHARTTIME"]
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

    # This implements the derived variables as shown in Table 3.8 of Caleb's thesis as well as Table 3.9
    def derived_variables(self, attributes, patient_measurements):
 
        attributes.append('vasopressorsm')
        attributes.append('pressorsm')
        attributes.append('sedativesm')
        attributes.append('cvpm')
        attributes.append('cotdm')
        attributes.append('pcwpm')
        attributes.append('crdindxm')
        attributes.append('papmeanm')
        attributes.append('hctm')
        attributes.append('lactatem')
        attributes.append('mechvent')
        attributes.append('sbpm')
        attributes.append('dbpm')
        attributes.append('mapm')
        attributes.append('pulsepres')
        attributes.append('eco')
        attributes.append('shockidx')
        attributes.append('buntocr')
        attributes.append('pao2tofio2')
        attributes.append('dopsm')
        attributes.append('dopmd')
        attributes.append('doplg')
        attributes.append('ventlen')
        attributes.append('ventlenc')
        attributes.append('pressortime')
        attributes.append('cumpressortime')
        attributes.append('sbpm.pr')
        attributes.append('mapm.pr')
        attributes.append('pressd01')
        attributes.append('pressd12')
        attributes.append('pressd24')
        attributes.append('pressd4')
        attributes.append('pressorcnt')
        attributes.append('vasopressorsum.std')
        attributes.append('pressorsum.std')
        attributes.append('bpcor')
        attributes.append('ecoslope')
        attributes.append('urinebyhour')
 
        nr_derived_vars = 38  # tables 3.8 and 3.9 from Caleb's thesis
        new_row = np.zeros(shape=(len(patient_measurements),nr_derived_vars))
         
        contiguous_ventlen = 0
        contiguous_pressor_time = 0
        sum_sbp_on_vasopressor = 0
        sum_sbp_not_on_vasopressor = 0
        occurences_sbp_on_vasopressor = 0
        occurences_sbp_not_on_vasopressor = 0
        sum_map_on_vasopressor = 0
        sum_map_not_on_vasopressor = 0
        occurences_map_on_vasopressor = 0
        occurences_map_not_on_vasopressor = 0
        time_since_admission = 0
        vasopressinBoolean = 0
        neosynephrineBoolean = 0
        levophedBoolean = 0
        dopamineBoolean = 0
        epinephrineBoolean = 0
         
        vasopressin_mean_dose   = np.mean([float(i) for i in patient_measurements[:,attributes.index('vasopressin')] if i.strip() is not '' or i.strip() is not 0])
        neosynephrine_mean_dose = np.mean([float(i) for i in patient_measurements[:,attributes.index('neosynephrine')] if i.strip() is not '' or i.strip() is not 0])
        levophed_mean_dose      = np.mean([float(i) for i in patient_measurements[:,attributes.index('levophed')] if i.strip() is not '' or i.strip() is not 0])
        dopamine_mean_dose      = np.mean([float(i) for i in patient_measurements[:,attributes.index('dopamine')] if i.strip() is not '' or i.strip() is not 0])
        epinephrine_mean_dose   = np.mean([float(i) for i in patient_measurements[:,attributes.index('epinephrine')] if i.strip() is not '' or i.strip() is not 0])
        dobutamine_mean_dose    = np.mean([float(i) for i in patient_measurements[:,attributes.index('dobutamine')] if i.strip() is not '' or i.strip() is not 0])
        miltinone_mean_dose     = np.mean([float(i) for i in patient_measurements[:,attributes.index('miltinone')] if i.strip() is not '' or i.strip() is not 0])
        amrinone_mean_dose      = np.mean([float(i) for i in patient_measurements[:,attributes.index('amrinone')] if i.strip() is not '' or i.strip() is not 0])
 
        for i in range(0, len(patient_measurements)):
            vasopressin     = patient_measurements[i,attributes.index('vasopressin')].strip()
            neosynephrine   = patient_measurements[i,attributes.index('neosynephrine')].strip()
            levophed        = patient_measurements[i,attributes.index('levophed')].strip()
            dopamine        = patient_measurements[i,attributes.index('dopamine')].strip()
            epinephrine     = patient_measurements[i,attributes.index('epinephrine')].strip()
            dobutamine      = patient_measurements[i,attributes.index('dobutamine')].strip()
            miltinone       = patient_measurements[i,attributes.index('miltinone')].strip()
            amrinone        = patient_measurements[i,attributes.index('amrinone')].strip()
            propofol        = patient_measurements[i,attributes.index('propofol')].strip()
            pentobarbitol   = patient_measurements[i,attributes.index('pentobarbitol')].strip()
            ativan          = patient_measurements[i,attributes.index('ativan')].strip()
            midazolam       = patient_measurements[i,attributes.index('midazolam')].strip()
            ketamine        = patient_measurements[i,attributes.index('ketamine')].strip()
            dilaudid        = patient_measurements[i,attributes.index('dilaudid')].strip()
            fentanyl        = patient_measurements[i,attributes.index('fentanyl')].strip()
            morphinesolfate = patient_measurements[i,attributes.index('morphinesolfate')].strip()
 
            #vasopressorsm
            if vasopressin == 0 and neosynephrine == 0 and levophed == 0 and dopamine == 0 and epinephrine == 0 :
                    new_row[i,0] = 0
            else:
                    new_row[i,0] = 1
            
            #pressorsm
            if vasopressin == 0 and neosynephrine == 0 and levophed == 0 and dopamine == 0 and epinephrine == 0  and dobutamine == 0 and miltinone == 0 and amrinone == 0:
                    new_row[i,1] = 0
            else:
                    new_row[i,1] = 1
            
            #sedativesm
            if propofol == 0 and midazolam == 0 and fentanyl == 0 and morphinesolfate == 0 and pentobarbitol == 0 and ativan == 0 and ketamine == 0 and dilaudid == 0:
                    new_row[i,2] = 0
            else:
                    new_row[i,2] = 1
            
            #cvpm
            new_row[i,3]  = float( patient_measurements[i,attributes.index('cvp')].strip() != 0 )   
            
            #cotdm
            new_row[i,4]  = float( patient_measurements[i,attributes.index('cotd')] != 0 )
            
            #pcwpm
            new_row[i,5]  = float( patient_measurements[i,attributes.index('pcwp')] != 0 )
            
            #crdindxm
            new_row[i,6]  = float( patient_measurements[i,attributes.index('crdindx')] != 0 )
            
            #papmeanm
            new_row[i,7]  = float( patient_measurements[i,attributes.index('papmean')] != 0 )
            
            #hctm
            new_row[i,8]  = float( patient_measurements[i,attributes.index('hct')] != 0 )
            
            #lactatem
            new_row[i,9]  = float( patient_measurements[i,attributes.index('lactate')]  != 0 )
            
            #mechvent
            new_row[i,10] = float(patient_measurements[i,attributes.index('vent_bin')] == 1)
            
            #sbpm, dbpm and mapm
            sbp     = patient_measurements[i,attributes.index('sbp')]
            dbp     = patient_measurements[i,attributes.index('dbp')]
            Map     = patient_measurements[i,attributes.index('map')]
            nbpsys  = patient_measurements[i,attributes.index('nbpsys')]
            nbpdias = patient_measurements[i,attributes.index('nbpdias')]
            nbpmean = patient_measurements[i,attributes.index('nbpmean')]
 
            if sbp is 0 and nbpsys.strip() is not 0:
                new_row[i,11] = nbpsys
            elif nbpsys.strip() is not '' and nbpdias.strip() is not '':
                if (sbp < 0.85*float(nbpsys) or dbp > 1.15*float(nbpdias)):
                    new_row[i,11] = nbpsys
 
            if dbp is 0 and nbpdias.strip() is not 0 and not '':
                new_row[i,12] = nbpdias
            elif nbpsys.strip() is not '' and nbpdias.strip() is not '' :
                if (sbp < 0.85*float(nbpsys) or dbp > 1.15*float(nbpdias)):
                    new_row[i,12] = nbpdias
 
            if Map is 0 and ( nbpmean.strip() is not 0 and not ''):
                new_row[i,13] = nbpmean
            elif nbpsys.strip() is not '' and nbpdias.strip() is not '' and nbpmean.strip() is not '':
                if (sbp < 0.85*float(nbpsys) or dbp > 1.15*float(nbpdias)):
                    new_row[i,13] = nbpmean
 
            #pulsepres
            new_row[i,14] = new_row[i,11] - new_row[i,12]
            
            #eco
            if (patient_measurements[i,attributes.index('hr')].strip() is not 0 and patient_measurements[i,attributes.index('hr')].strip() is not '') and new_row[i,13] != 0:
                new_row[i,15] = 0.5*(float(patient_measurements[i,attributes.index('hr')].strip())*(new_row[i,14]))/new_row[i,13]
            
            #shockidx
            if patient_measurements[i,attributes.index('hr')].strip() is not 0 and patient_measurements[i,attributes.index('hr')].strip() is not '':
                if new_row[i,11] != 0:
                    new_row[i,16] = float(patient_measurements[i,attributes.index('hr')])/new_row[i,11]
            
            #buntocr
            if (patient_measurements[i,attributes.index('bun')].strip() is not '' and patient_measurements[i,attributes.index('bun')].strip() is not 0) and (patient_measurements[i,attributes.index('creatinine')].strip() is not '' and not 0):
                new_row[i,17] = float(patient_measurements[i,attributes.index('bun')])/float(patient_measurements[i,attributes.index('creatinine')])
            
            #pao2tofio2
            if (patient_measurements[i,attributes.index('artpao2')].strip() is not '' and patient_measurements[i,attributes.index('artpao2')].strip() is not 0) and (patient_measurements[i,attributes.index('fio2set')].strip() is not '' and patient_measurements[i,attributes.index('fio2set')].strip() is not 0):
                ratio = float(patient_measurements[i,attributes.index('artpao2')])/float(patient_measurements[i,attributes.index('fio2set')])
                ventilated = new_row[i,10]
                if ventilated   == 0 or (ventilated == 1 and ratio >300):
                        new_row[i,18] = 0
                elif ventilated == 1 and 200 < ratio < 300:
                        new_row[i,18] = 1
                elif ventilated == 1 and ratio < 100:
                        new_row[i,18] = 2
            
            #dopsm, dopmd and doplg
            new_row[i,19] = float(patient_measurements[i,attributes.index('dopamine')].strip()<2)
            new_row[i,20] = float(2<patient_measurements[i,attributes.index('dopamine')].strip()<10)
            new_row[i,21] = float(patient_measurements[i,attributes.index('dopamine')].strip()>10)
            
            #ventlen
            ventilated = patient_measurements[i,attributes.index('vent_bin')]
            if i is not 0 and float(patient_measurements[i-1,attributes.index('vent_bin')]) == 1:
                contiguous_ventlen += (np.float64(patient_measurements[i,attributes.index('charttime')])-np.float(patient_measurements[i-1,attributes.index('charttime')]))*1.666666667*math.pow(10,-5)
                new_row[i,22] = contiguous_ventlen
            else:
                contiguous_ventlen = 0
                new_row[i,22] = contiguous_ventlen
            
            #ventlenc
            new_row[i,23] = np.sum(new_row[0:i-1,22])
            
            #pressortime
            contiguous_pressor_time
            if i is not 0 and new_row[i-1,0] == 1:
                contiguous_pressor_time += (np.float(patient_measurements[i,attributes.index('charttime')])-np.float(patient_measurements[i-1,attributes.index('charttime')]))*1.666666667*math.pow(10,-5)
                new_row[i,24] = contiguous_pressor_time
            else:
                contiguous_pressor_time = 0
                new_row[i,24] = contiguous_pressor_time
            
            #cumpressortime
            new_row[i,25] = np.sum(new_row[0:i-1,24])
            
            #sbpm.pr
            if new_row[i,0]   == 1 and new_row[i,11] is not 0:
                sum_sbp_on_vasopressor += float(new_row[i,11])
                occurences_sbp_on_vasopressor += 1
            elif new_row[i,0] == 0 and new_row[i,11] is not 0:
                sum_sbp_not_on_vasopressor += float(new_row[i,11])
                occurences_sbp_not_on_vasopressor += 1
 
            if sum_sbp_not_on_vasopressor != 0 and occurences_sbp_not_on_vasopressor != 0 and occurences_sbp_on_vasopressor != 0:
                new_row[i,26] = (sum_sbp_on_vasopressor / occurences_sbp_on_vasopressor) / (sum_sbp_not_on_vasopressor/occurences_sbp_not_on_vasopressor)
            
            #mapm.pr
            if new_row[i,0] == 1 and new_row[i,13] is not 0:
                sum_map_on_vasopressor += float(new_row[i,13])
                occurences_map_on_vasopressor += 1
            elif new_row[i,0] == 0 and new_row[i,13] is not 0:
                sum_map_not_on_vasopressor += float(new_row[i,13])
                occurences_map_not_on_vasopressor += 1
            if sum_map_not_on_vasopressor != 0 and occurences_map_not_on_vasopressor != 0 and occurences_map_on_vasopressor != 0:
                new_row[i,27] = np.nan_to_num( (sum_map_on_vasopressor / occurences_map_on_vasopressor) / (sum_map_not_on_vasopressor/occurences_map_not_on_vasopressor))
            
            #pressd01, pressd12, pressd24, pressd4
            time_since_admission =  (np.float(patient_measurements[i,attributes.index('charttime')])-np.float(patient_measurements[i-1,attributes.index('charttime')]))*1.666666667*math.pow(10,-5)
            if  (time_since_admission <= 24*60 and new_row[i,0] == 1) or sum(new_row[0:i,28])>0  :
                    new_row[i,28] = 1
                    new_row[i,29] = 0
                    new_row[i,30] = 0
                    new_row[i,31] = 0
            elif (24*60 < time_since_admission <= 24*60*2 and sum(new_row[0:i,28]) == 0 and new_row[i,0] == 1) or sum(new_row[0:i,29])>0 :
                    new_row[i,28] = 0
                    new_row[i,29] = 1
                    new_row[i,30] = 0
                    new_row[i,31] = 0
            elif (24*60*2 < time_since_admission <= 24*60*4 and sum(new_row[0:i,28]) == 0 and sum(new_row[0:i,29]) == 0 and new_row[i,0] == 1) or sum(new_row[0:i,30])>0:
                    new_row[i,28] = 0
                    new_row[i,29] = 0
                    new_row[i,30] = 1
                    new_row[i,31] = 0
            elif (time_since_admission > 24*60*4 and sum(new_row[0:i,28]) == 0 and sum(new_row[0:i,29]) == 0 and sum(new_row[0:i,30]) == 0 and new_row[i,0] == 1) or sum(new_row[0:i,31])>0:
                    new_row[i,28] = 0
                    new_row[i,29] = 0
                    new_row[i,30] = 0
                    new_row[i,31] = 1
            
            #pressorcnt
            if vasopressin is not 0 :
                    vasopressinBoolean   = 1  
            if neosynephrine is not 0 :
                    neosynephrineBoolean = 1
            if dopamine is not 0 :
                    dopamineBoolean      = 1
            if levophed is not 0 :
                    levophedBoolean      = 1
            if epinephrine is not 0 :
                    epinephrineBoolean   = 1
 
            new_row[i,32] = float(vasopressinBoolean)+ float(neosynephrineBoolean ) + float( dopamineBoolean  ) + float( levophedBoolean ) + float( epinephrineBoolean)    
            
            #vasopressorsum.std and pressorsum.std
            value=0
            value2=0
            if (vasopressin_mean_dose is not np.nan and vasopressin_mean_dose != 0) and (vasopressin is not 0 and vasopressin is not ''):
                value += float(patient_measurements[i,attributes.index('vasopressin')])/vasopressin_mean_dose
            if (neosynephrine_mean_dose is not np.nan and neosynephrine_mean_dose != 0) and  (neosynephrine is not 0 and neosynephrine is not ''):
                value += float(patient_measurements[i,attributes.index('neosynephrine')])/neosynephrine_mean_dose
            if (levophed_mean_dose is not np.nan and levophed_mean_dose != 0) and (levophed is not 0 and levophed is not ''):
                value += float(patient_measurements[i,attributes.index('levophed')])/levophed_mean_dose
            if (dopamine_mean_dose is not np.nan and dopamine_mean_dose != 0) and (dopamine is not 0 and dopamine is not ''):
                value += float(patient_measurements[i,attributes.index('dopamine')])/dopamine_mean_dose
            if (epinephrine_mean_dose is not np.nan and epinephrine_mean_dose != 0) and (epinephrine is not 0 and epinephrine is not ''):
                value += float(patient_measurements[i,attributes.index('epinephrine')])/epinephrine_mean_dose
            value2 = value
            if (dobutamine_mean_dose is not np.nan and dobutamine_mean_dose != 0) and (dobutamine is not 0 and dobutamine is not ''):
                value2 += float(patient_measurements[i,attributes.index('dobutamine')])/dobutamine_mean_dose
            if (miltinone_mean_dose is not np.nan and miltinone_mean_dose != 0) and (miltinone is not 0 and miltinone is not ''):
                value2 += float(patient_measurements[i,attributes.index('miltinone')])/miltinone_mean_dose
            if (amrinone_mean_dose is not np.nan and amrinone_mean_dose != 0) and (amrinone is not 0 and amrinone is not ''):
                value2 += float(patient_measurements[i,attributes.index('amrinone')])/amrinone_mean_dose   
 
            if value is not np.nan :
                new_row[i,33] = value
            if value2 is not np.nan :
                new_row[i,34] = value2    
            
            #bpcor 
            array1 = [float(j) for j in patient_measurements[0:i,attributes.index('map')] if j.strip() is not '']
            array2 = []
            for k in range(0,len(patient_measurements[0:i,attributes.index('map')])) :
                if patient_measurements[k,attributes.index('map')].strip() is not '':
                    array2.append(float(new_row[k,34]))
            new_row[i,35] = np.nan_to_num(np.corrcoef(array1,array2)[0,1])
            
            #ecoslope
            if i >= 6*4 :
                new_row[i,36] = self.perform_regression([patient_measurements[p,attributes.index('charttime')] for p in range(i-(6*4),i)], [new_row[p,15] for p in range(i-(6*4),i)])
            else:
                new_row[i,36] = self.perform_regression(patient_measurements[0:i,attributes.index('charttime')] , new_row[0:i,15])
            
            #urinebyhr   urineout
            if patient_measurements[i,attributes.index('urineout')].strip() == 0 or patient_measurements[i,attributes.index('urineout')].strip() is '':
                new_row[i,37] = 0
            else:
                if i == 0 :
                        new_row[i,37] = 4*float(patient_measurements[i,attributes.index('urineout')])
                if i == 1 :
                        new_row[i,37] = 4*float(patient_measurements[i,attributes.index('urineout')]) #+ patient_measurements[i-1,attributes.index('urineout')]
                if i == 2 :
                        new_row[i,37] = 4*float(patient_measurements[i,attributes.index('urineout')]) #+sum(new_row[i-2:i-1,37]) 
                if i >= 3 :
                    v = 0
                    for t in range(i-4,i):
                        if patient_measurements[t,attributes.index('urineout')].strip() is not 0 and patient_measurements[t,attributes.index('urineout')].strip() is not '':
                            v += float(patient_measurements[t,attributes.index('urineout')])
                        new_row[i,37] = v   
        
        
        patient_measurements = np.append(patient_measurements,  new_row, axis=1)
        temp = np.copy(patient_measurements[:,attributes.index('label')])
        patient_measurements[:,attributes.index('label')] = np.copy(patient_measurements[:,attributes.index('urinebyhour')])
        patient_measurements[:,attributes.index('urinebyhour')] = temp
        
        # make sure 'label' is the last column
        label_index = attributes.index('label')
        new_index = attributes.index('urinebyhour')
        attributes[label_index] = 'urinebyhour'
        attributes[new_index] = 'label'      
        return [attributes, patient_measurements]
    
    # This function performs medication scaling as shown in Table 3.3 of Caleb's thesis 
    def medication_scaling(self,attributes,patient_measurements):
        nr_MedEvent_vars = 50 
        new_row = np.zeros(shape=(len(patient_measurements),nr_MedEvent_vars*3))
        
        for medication in self.medication_features:
            attributes.append(medication + '_absolute')
            attributes.append(medication + '_per_kg')
            attributes.append(medication + '_agent')
            
        p = 0
        weight_first = 0 
        while (p < len(patient_measurements) and (weight_first is '' or weight_first is 0)):
            if patient_measurements[p,attributes.index('weight_first')].strip() is not '' and patient_measurements[p,attributes.index('weight_first')].strip() is not 0:
                weight_first = float(patient_measurements[p,attributes.index('weight_first')].strip())
                
                #print weight_first
                break;
            p += 1
        for i in range(0, len(patient_measurements)):
            print weight_first
            if weight_first == 0 :
                    new_row[i,:] = np.float(0)
            else:
                    # aggrastat 
                    if patient_measurements[i,attributes.index('aggrestat')].strip() is not 0 and patient_measurements[i,attributes.index('aggrestat')].strip() is not '':
                        new_row[i,0] = float(patient_measurements[i,attributes.index('aggrestat')].strip())* weight_first
                        new_row[i,1] = float(patient_measurements[i,attributes.index('aggrestat')].strip())
                        print new_row[i,1]
                        print float(patient_measurements[i,attributes.index('aggrestat')].strip())
                        new_row[i,2] = float(patient_measurements[i,attributes.index('aggrestat')].strip())
                    # vecuronium
                    if patient_measurements[i,attributes.index('vecuronium')].strip() is not 0 and patient_measurements[i,attributes.index('vecuronium')].strip() is not '':
                        new_row[i,3] = float(patient_measurements[i,attributes.index('vecuronium')].strip()) * weight_first
                        new_row[i,4] = float(patient_measurements[i,attributes.index('vecuronium')].strip())
                        new_row[i,5] = float(patient_measurements[i,attributes.index('vecuronium')].strip()) 
                    # aminophylline
                    if patient_measurements[i,attributes.index('aminophylline')].strip() is not 0 and patient_measurements[i,attributes.index('aminophylline')].strip() is not '':
                        new_row[i,6] = float(patient_measurements[i,attributes.index('aminophylline')].strip())
                        new_row[i,7] = float(patient_measurements[i,attributes.index('aminophylline')].strip()) / weight_first
                        new_row[i,8] = float(patient_measurements[i,attributes.index('aminophylline')].strip())
                    # amiodarone
                    if patient_measurements[i,attributes.index('amiodarone')].strip() is not 0 and patient_measurements[i,attributes.index('amiodarone')].strip() is not '':
                        new_row[i,9] = float(patient_measurements[i,attributes.index('amiodarone')].strip())
                        new_row[i,10] = float(patient_measurements[i,attributes.index('amiodarone')].strip()) / weight_first
                        new_row[i,11] = float(patient_measurements[i,attributes.index('amiodarone')].strip())
                    # amrinone
                    if patient_measurements[i,attributes.index('amrinone')].strip() is not 0 and patient_measurements[i,attributes.index('amrinone')].strip() is not '':
                        new_row[i,12] = float(patient_measurements[i,attributes.index('amrinone')].strip())* weight_first 
                        new_row[i,13] = float(patient_measurements[i,attributes.index('amrinone')].strip())
                        new_row[i,14] = float(patient_measurements[i,attributes.index('amrinone')].strip())
                    # argatroban
                    if patient_measurements[i,attributes.index('argatroban')].strip() is not 0 and patient_measurements[i,attributes.index('argatroban')].strip() is not '':
                        new_row[i,15] = float(patient_measurements[i,attributes.index('argatroban')].strip()) * weight_first 
                        new_row[i,16] = float(patient_measurements[i,attributes.index('argatroban')].strip())
                        new_row[i,17] = float(patient_measurements[i,attributes.index('argatroban')].strip())
                    # ativan
                    if patient_measurements[i,attributes.index('ativan')].strip() is not 0 and patient_measurements[i,attributes.index('ativan')].strip() is not '':
                        new_row[i,18] = float(patient_measurements[i,attributes.index('ativan')].strip())  
                        new_row[i,19] = float(patient_measurements[i,attributes.index('ativan')].strip())/ weight_first
                        new_row[i,20] = float(patient_measurements[i,attributes.index('ativan')].strip())
                    # atracurium
                    if patient_measurements[i,attributes.index('atracurium')].strip() is not 0 and patient_measurements[i,attributes.index('atracurium')].strip() is not '':
                        new_row[i,21] = float(patient_measurements[i,attributes.index('atracurium')].strip()) *  weight_first
                        new_row[i,22] = float(patient_measurements[i,attributes.index('atracurium')].strip()) 
                        new_row[i,23] = float(patient_measurements[i,attributes.index('atracurium')].strip())
                    # bivalirudin
                    if patient_measurements[i,attributes.index('bivalirudin')].strip() is not 0 and patient_measurements[i,attributes.index('bivalirudin')].strip() is not '':
                        new_row[i,24] = float(patient_measurements[i,attributes.index('bivalirudin')].strip()) *  weight_first
                        new_row[i,25] = float(patient_measurements[i,attributes.index('bivalirudin')].strip()) 
                        new_row[i,26] = float(patient_measurements[i,attributes.index('bivalirudin')].strip())
                    # vasopressin
                    if patient_measurements[i,attributes.index('vasopressin')].strip() is not 0 and patient_measurements[i,attributes.index('vasopressin')].strip() is not '':
                        new_row[i,27] = float(patient_measurements[i,attributes.index('vasopressin')].strip()) 
                        new_row[i,28] = float(patient_measurements[i,attributes.index('vasopressin')].strip()) / weight_first
                        new_row[i,29] = float(patient_measurements[i,attributes.index('vasopressin')].strip()) 
                    # dilaudid
                    if patient_measurements[i,attributes.index('dilaudid')].strip() is not 0 and patient_measurements[i,attributes.index('dilaudid')].strip() is not '':
                        new_row[i,30] = float(patient_measurements[i,attributes.index('dilaudid')].strip()) 
                        new_row[i,31] = float(patient_measurements[i,attributes.index('dilaudid')].strip()) / weight_first
                        new_row[i,32] = float(patient_measurements[i,attributes.index('dilaudid')].strip())
                    # diltiazem
                    if patient_measurements[i,attributes.index('diltiazem')].strip() is not 0 and patient_measurements[i,attributes.index('diltiazem')].strip() is not '':
                        new_row[i,33] = float(patient_measurements[i,attributes.index('diltiazem')].strip()) 
                        new_row[i,34] = float(patient_measurements[i,attributes.index('diltiazem')].strip()) / weight_first
                        new_row[i,35] = float(patient_measurements[i,attributes.index('diltiazem')].strip())
                    # dobutamine
                    if patient_measurements[i,attributes.index('dobutamine')].strip() is not 0 and patient_measurements[i,attributes.index('dobutamine')].strip() is not '':
                        new_row[i,36] = float(patient_measurements[i,attributes.index('dobutamine')].strip()) * weight_first
                        new_row[i,37] = float(patient_measurements[i,attributes.index('dobutamine')].strip()) 
                        new_row[i,38] = float(patient_measurements[i,attributes.index('dobutamine')].strip())
                    # dopamine
                    if patient_measurements[i,attributes.index('dopamine')].strip() is not 0 and patient_measurements[i,attributes.index('dopamine')].strip() is not '':
                        new_row[i,39] = float(patient_measurements[i,attributes.index('dopamine')].strip()) * weight_first
                        new_row[i,40] = float(patient_measurements[i,attributes.index('dopamine')].strip()) 
                        new_row[i,41] = float(patient_measurements[i,attributes.index('dopamine')].strip())
                    # doxacurium
                    if patient_measurements[i,attributes.index('doxacurium')].strip() is not 0 and patient_measurements[i,attributes.index('doxacurium')].strip() is not '':
                        new_row[i,42] = float(patient_measurements[i,attributes.index('doxacurium')].strip()) * weight_first
                        new_row[i,43] = float(patient_measurements[i,attributes.index('doxacurium')].strip()) 
                        new_row[i,44] = float(patient_measurements[i,attributes.index('doxacurium')].strip())
                    # epinephrine
                    if patient_measurements[i,attributes.index('epinephrine')].strip() is not 0 and patient_measurements[i,attributes.index('epinephrine')].strip() is not '':
                        new_row[i,45] = float(patient_measurements[i,attributes.index('epinephrine')].strip()) 
                        new_row[i,46] = float(patient_measurements[i,attributes.index('epinephrine')].strip()) / weight_first
                        new_row[i,47] = float(patient_measurements[i,attributes.index('epinephrine')].strip())
                    # epinephrine-k
                    if patient_measurements[i,attributes.index('epinephrinek')].strip() is not 0 and patient_measurements[i,attributes.index('epinephrinek')].strip() is not '':
                        new_row[i,48] = float(patient_measurements[i,attributes.index('epinephrinek')].strip()) * weight_first
                        new_row[i,49] = float(patient_measurements[i,attributes.index('epinephrinek')].strip()) 
                        new_row[i,50] = float(patient_measurements[i,attributes.index('epinephrinek')].strip())
                    # tpa
                    if patient_measurements[i,attributes.index('tpa')].strip() is not 0 and patient_measurements[i,attributes.index('tpa')].strip() is not '':
                        new_row[i,51] = float(patient_measurements[i,attributes.index('tpa')].strip()) 
                        new_row[i,52] = float(patient_measurements[i,attributes.index('tpa')].strip()) / weight_first
                        new_row[i,53] = float(patient_measurements[i,attributes.index('tpa')].strip())
                    # fentanylconc
                    if patient_measurements[i,attributes.index('fentanylconc')].strip() is not 0 and patient_measurements[i,attributes.index('fentanylconc')].strip() is not '':
                        new_row[i,54] = float(patient_measurements[i,attributes.index('fentanylconc')].strip()) 
                        new_row[i,55] = float(patient_measurements[i,attributes.index('fentanylconc')].strip()) / weight_first
                        new_row[i,56] = float(patient_measurements[i,attributes.index('fentanylconc')].strip())
                    # fentanyl
                    if patient_measurements[i,attributes.index('fentanyl')].strip() is not 0 and patient_measurements[i,attributes.index('fentanyl')].strip() is not '':
                        new_row[i,57] = float(patient_measurements[i,attributes.index('fentanyl')].strip()) 
                        new_row[i,58] = float(patient_measurements[i,attributes.index('fentanyl')].strip()) / weight_first
                        new_row[i,59] = float(patient_measurements[i,attributes.index('fentanyl')].strip())
                    # heparin
                    if patient_measurements[i,attributes.index('heparin')].strip() is not 0 and patient_measurements[i,attributes.index('heparin')].strip() is not '':
                        new_row[i,60] = float(patient_measurements[i,attributes.index('heparin')].strip()) 
                        new_row[i,61] = float(patient_measurements[i,attributes.index('heparin')].strip()) / weight_first
                        new_row[i,62] = float(patient_measurements[i,attributes.index('heparin')].strip())
                    # insulin
                    if patient_measurements[i,attributes.index('insulin')].strip() is not 0 and patient_measurements[i,attributes.index('insulin')].strip() is not '':
                        new_row[i,63] = float(patient_measurements[i,attributes.index('insulin')].strip()) 
                        new_row[i,64] = float(patient_measurements[i,attributes.index('insulin')].strip()) / weight_first
                        new_row[i,65] = float(patient_measurements[i,attributes.index('insulin')].strip())
                    # integrelin
                    if patient_measurements[i,attributes.index('integrelin')].strip() is not 0 and patient_measurements[i,attributes.index('integrelin')].strip() is not '':
                        new_row[i,66] = float(patient_measurements[i,attributes.index('integrelin')].strip()) * weight_first
                        new_row[i,67] = float(patient_measurements[i,attributes.index('integrelin')].strip()) 
                        new_row[i,68] = float(patient_measurements[i,attributes.index('integrelin')].strip())
                    # ketamine
                    if patient_measurements[i,attributes.index('ketamine')].strip() is not 0 and patient_measurements[i,attributes.index('ketamine')].strip() is not '':
                        new_row[i,69] = float(patient_measurements[i,attributes.index('ketamine')].strip()) * weight_first
                        new_row[i,70] = float(patient_measurements[i,attributes.index('ketamine')].strip()) 
                        new_row[i,71] = float(patient_measurements[i,attributes.index('ketamine')].strip())
                    # labetolol
                    if patient_measurements[i,attributes.index('labetolol')].strip() is not 0 and patient_measurements[i,attributes.index('labetolol')].strip() is not '':
                        new_row[i,72] = float(patient_measurements[i,attributes.index('labetolol')].strip()) 
                        new_row[i,73] = float(patient_measurements[i,attributes.index('labetolol')].strip()) / weight_first
                        new_row[i,74] = float(patient_measurements[i,attributes.index('labetolol')].strip())   
                    # lasix
                    if patient_measurements[i,attributes.index('lasix')].strip() is not 0 and patient_measurements[i,attributes.index('lasix')].strip() is not '':
                        new_row[i,75] = float(patient_measurements[i,attributes.index('lasix')].strip()) 
                        new_row[i,76] = float(patient_measurements[i,attributes.index('lasix')].strip()) / weight_first
                        new_row[i,77] = float(patient_measurements[i,attributes.index('lasix')].strip())
                    # lepirudin
                    if patient_measurements[i,attributes.index('lepirudin')].strip() is not 0 and patient_measurements[i,attributes.index('lepirudin')].strip() is not '':
                        new_row[i,78] = float(patient_measurements[i,attributes.index('lepirudin')].strip()) * weight_first
                        new_row[i,79] = float(patient_measurements[i,attributes.index('lepirudin')].strip())
                        new_row[i,80] = float(patient_measurements[i,attributes.index('lepirudin')].strip())    
                    # levophed
                    if patient_measurements[i,attributes.index('levophed')].strip() is not 0 and patient_measurements[i,attributes.index('levophed')].strip() is not '':
                        new_row[i,81] = float(patient_measurements[i,attributes.index('levophed')].strip()) 
                        new_row[i,82] = float(patient_measurements[i,attributes.index('levophed')].strip()) / weight_first
                        new_row[i,83] = float(patient_measurements[i,attributes.index('levophed')].strip())
                    # levophed-k
                    if patient_measurements[i,attributes.index('levophedk')].strip() is not 0 and patient_measurements[i,attributes.index('levophedk')].strip() is not '':
                        new_row[i,84] = float(patient_measurements[i,attributes.index('levophedk')].strip()) * weight_first
                        new_row[i,85] = float(patient_measurements[i,attributes.index('levophedk')].strip())
                        new_row[i,86] = float(patient_measurements[i,attributes.index('levophedk')].strip())  
                    # lidocaine
                    if patient_measurements[i,attributes.index('lidocaine')].strip() is not 0 and patient_measurements[i,attributes.index('lidocaine')].strip() is not '':
                        new_row[i,87] = float(patient_measurements[i,attributes.index('lidocaine')].strip()) 
                        new_row[i,88] = float(patient_measurements[i,attributes.index('lidocaine')].strip()) / weight_first
                        new_row[i,89] = float(patient_measurements[i,attributes.index('lidocaine')].strip())  
                    # midazolam
                    if patient_measurements[i,attributes.index('midazolam')].strip() is not 0 and patient_measurements[i,attributes.index('midazolam')].strip() is not '':
                        new_row[i,90] = float(patient_measurements[i,attributes.index('midazolam')].strip()) 
                        new_row[i,91] = float(patient_measurements[i,attributes.index('midazolam')].strip()) / weight_first
                        new_row[i,92] = float(patient_measurements[i,attributes.index('midazolam')].strip())  
                    # miltinone
                    if patient_measurements[i,attributes.index('miltinone')].strip() is not 0 and patient_measurements[i,attributes.index('miltinone')].strip() is not '':
                        new_row[i,93] = float(patient_measurements[i,attributes.index('miltinone')].strip()) * weight_first
                        new_row[i,94] = float(patient_measurements[i,attributes.index('miltinone')].strip())
                        new_row[i,95] = float(patient_measurements[i,attributes.index('miltinone')].strip())  
                    # morphinesolfate
                    if patient_measurements[i,attributes.index('morphinesolfate')].strip() is not 0 and patient_measurements[i,attributes.index('morphinesolfate')].strip() is not '':
                        new_row[i,96] = float(patient_measurements[i,attributes.index('morphinesolfate')].strip()) 
                        new_row[i,97] = float(patient_measurements[i,attributes.index('morphinesolfate')].strip()) / weight_first
                        new_row[i,98] = float(patient_measurements[i,attributes.index('morphinesolfate')].strip()) 
                    # narcan
                    if patient_measurements[i,attributes.index('narcan')].strip() is not 0 and patient_measurements[i,attributes.index('narcan')].strip() is not '':
                        new_row[i,99] = float(patient_measurements[i,attributes.index('narcan')].strip()) * weight_first
                        new_row[i,100] = float(patient_measurements[i,attributes.index('narcan')].strip())
                        new_row[i,101] = float(patient_measurements[i,attributes.index('narcan')].strip()) 
                    # natrecor
                    if patient_measurements[i,attributes.index('natrecor')].strip() is not 0 and patient_measurements[i,attributes.index('natrecor')].strip() is not '':
                        new_row[i,102] = float(patient_measurements[i,attributes.index('natrecor')].strip()) * weight_first
                        new_row[i,103] = float(patient_measurements[i,attributes.index('natrecor')].strip())
                        new_row[i,104] = float(patient_measurements[i,attributes.index('natrecor')].strip())  
                    # neosynephrine
                    if patient_measurements[i,attributes.index('neosynephrine')].strip() is not 0 and patient_measurements[i,attributes.index('neosynephrine')].strip() is not '':
                        new_row[i,105] = float(patient_measurements[i,attributes.index('neosynephrine')].strip()) 
                        new_row[i,106] = float(patient_measurements[i,attributes.index('neosynephrine')].strip()) / weight_first
                        new_row[i,107] = float(patient_measurements[i,attributes.index('neosynephrine')].strip())  
                    # neosynephrine-k
                    if patient_measurements[i,attributes.index('neosynephrinek')].strip() is not 0 and patient_measurements[i,attributes.index('neosynephrinek')].strip() is not '':
                        new_row[i,108] = float(patient_measurements[i,attributes.index('neosynephrinek')].strip()) * weight_first
                        new_row[i,109] = float(patient_measurements[i,attributes.index('neosynephrinek')].strip())
                        new_row[i,110] = float(patient_measurements[i,attributes.index('neosynephrinek')].strip()) 
                    # nicardipine
                    if patient_measurements[i,attributes.index('nicardipine')].strip() is not 0 and patient_measurements[i,attributes.index('nicardipine')].strip() is not '':
                        new_row[i,111] = float(patient_measurements[i,attributes.index('nicardipine')].strip()) * weight_first
                        new_row[i,112] = float(patient_measurements[i,attributes.index('nicardipine')].strip())
                        new_row[i,113] = float(patient_measurements[i,attributes.index('nicardipine')].strip())
                    # nitroglycerine
                    if patient_measurements[i,attributes.index('nitroglycerine')].strip() is not 0 and patient_measurements[i,attributes.index('nitroglycerine')].strip() is not '':
                        new_row[i,114] = float(patient_measurements[i,attributes.index('nitroglycerine')].strip()) 
                        new_row[i,115] = float(patient_measurements[i,attributes.index('nitroglycerine')].strip()) / weight_first
                        new_row[i,116] = float(patient_measurements[i,attributes.index('nitroglycerine')].strip())  
                    # nitroglycerine-k
                    if patient_measurements[i,attributes.index('nitroglycerinek')].strip() is not 0 and patient_measurements[i,attributes.index('nitroglycerinek')].strip() is not '':
                        new_row[i,117] = float(patient_measurements[i,attributes.index('nitroglycerinek')].strip()) * weight_first
                        new_row[i,118] = float(patient_measurements[i,attributes.index('nitroglycerinek')].strip())
                        new_row[i,119] = float(patient_measurements[i,attributes.index('nitroglycerinek')].strip())
                    # nitroprusside
                    if patient_measurements[i,attributes.index('nitroprusside')].strip() is not 0 and patient_measurements[i,attributes.index('nitroprusside')].strip() is not '':
                        new_row[i,120] = float(patient_measurements[i,attributes.index('nitroprusside')].strip())* weight_first
                        new_row[i,121] = float(patient_measurements[i,attributes.index('nitroprusside')].strip())
                        new_row[i,122] = float(patient_measurements[i,attributes.index('nitroprusside')].strip())
                    # pancuronium
                    if patient_measurements[i,attributes.index('pancuronium')].strip() is not 0 and patient_measurements[i,attributes.index('pancuronium')].strip() is not '':
                        new_row[i,123] = float(patient_measurements[i,attributes.index('pancuronium')].strip()) * weight_first
                        new_row[i,124] = float(patient_measurements[i,attributes.index('pancuronium')].strip())
                        new_row[i,125] = float(patient_measurements[i,attributes.index('pancuronium')].strip())
                    # pentobarbitol
                    if patient_measurements[i,attributes.index('pentobarbitol')].strip() is not 0 and patient_measurements[i,attributes.index('pentobarbitol')].strip() is not '':
                        new_row[i,126] = float(patient_measurements[i,attributes.index('pentobarbitol')].strip()) * weight_first
                        new_row[i,127] = float(patient_measurements[i,attributes.index('pentobarbitol')].strip())
                        new_row[i,128] = float(patient_measurements[i,attributes.index('pentobarbitol')].strip())
                    # precedex
                    if patient_measurements[i,attributes.index('precedex')].strip() is not 0 and patient_measurements[i,attributes.index('precedex')].strip() is not '':
                        new_row[i,129] = float(patient_measurements[i,attributes.index('precedex')].strip()) * weight_first
                        new_row[i,130] = float(patient_measurements[i,attributes.index('precedex')].strip())
                        new_row[i,131] = float(patient_measurements[i,attributes.index('precedex')].strip())
                    # procainamide
                    if patient_measurements[i,attributes.index('procainamide')].strip() is not 0 and patient_measurements[i,attributes.index('procainamide')].strip() is not '':
                        new_row[i,132] = float(patient_measurements[i,attributes.index('procainamide')].strip()) 
                        new_row[i,133] = float(patient_measurements[i,attributes.index('procainamide')].strip()) / weight_first
                        new_row[i,134] = float(patient_measurements[i,attributes.index('procainamide')].strip())  
                    # propofol
                    if patient_measurements[i,attributes.index('propofol')].strip() is not 0 and patient_measurements[i,attributes.index('propofol')].strip() is not '':
                        new_row[i,135] = float(patient_measurements[i,attributes.index('propofol')].strip()) * weight_first
                        new_row[i,136] = float(patient_measurements[i,attributes.index('propofol')].strip())
                        new_row[i,137] = float(patient_measurements[i,attributes.index('propofol')].strip()) 
                    # reopro
                    if patient_measurements[i,attributes.index('reopro')].strip() is not 0 and patient_measurements[i,attributes.index('reopro')].strip() is not '':
                        new_row[i,138] = float(patient_measurements[i,attributes.index('reopro')].strip()) * weight_first
                        new_row[i,139] = float(patient_measurements[i,attributes.index('reopro')].strip())
                        new_row[i,140] = float(patient_measurements[i,attributes.index('reopro')].strip()) 
                    # sandostatin
                    if patient_measurements[i,attributes.index('sandostatin')].strip() is not 0 and patient_measurements[i,attributes.index('sandostatin')].strip() is not '':
                        new_row[i,141] = float(patient_measurements[i,attributes.index('sandostatin')].strip()) 
                        new_row[i,142] = float(patient_measurements[i,attributes.index('sandostatin')].strip()) / weight_first
                        new_row[i,143] = float(patient_measurements[i,attributes.index('sandostatin')].strip()) 
                    # cistracurium 
                    if patient_measurements[i,attributes.index('cistracurium')].strip() is not 0 and patient_measurements[i,attributes.index('cistracurium')].strip() is not '':
                        new_row[i,144] = float(patient_measurements[i,attributes.index('cistracurium')].strip())*  weight_first
                        new_row[i,145] = float(patient_measurements[i,attributes.index('cistracurium')].strip()) 
                        new_row[i,146] = float(patient_measurements[i,attributes.index('cistracurium')].strip())
                    # esmolol 
                    if patient_measurements[i,attributes.index('esmolol')].strip() is not 0 and patient_measurements[i,attributes.index('esmolol')].strip() is not '':
                        new_row[i,147] = float(patient_measurements[i,attributes.index('esmolol')].strip()) * weight_first
                        new_row[i,148] = float(patient_measurements[i,attributes.index('esmolol')].strip()) 
                        new_row[i,149] = float(patient_measurements[i,attributes.index('esmolol')].strip()) 
                    
        #             # amicar  (not in dataset)
        #             if patient_measurements[i,attributes.index('amicar')].strip() is not 0 or if patient_measurements[i,attributes.index('amicar')].strip() is not '':
        #                 new_row[i,3] = patient_measurements[i,attributes.index('amicar')].strip()
        #                 new_row[i,4] = patient_measurements[i,attributes.index('amicar')].strip() / weight_first
        #                 new_row[i,5] = float(patient_measurements[i,attributes.index('amicar')].strip())
             
        patient_measurements = np.append(patient_measurements,  new_row, axis=1)        
        print new_row
        temp = np.copy(patient_measurements[:,attributes.index('label')])
        patient_measurements[:,attributes.index('label')] = np.copy(patient_measurements[:,len(attributes)-1])
        patient_measurements[:,len(attributes)-1] = temp
        
        # make sure 'label' is the last column
        label_index = attributes.index('label')
        new_index = len(attributes)-1
        attributes[label_index] = attributes[len(attributes)-1]
        attributes[new_index] = 'label'           
         
        return [attributes, patient_measurements]
    
    # Perform sample-and-hold appraoch
    def perform_hold_approach(self, attributes, patient_measurements):
        print "performing hold"
        
        #28*4
        set1 = ["GCS", "Weight","AdmitWt","Na","K","Cl","CO2","Glucose","BUN","Creatinine","Mg","AST","ALT","Ca","IonCa",\
                "TBili","DBili","TProtein","Albumin","Lactate","Troponin","HCT","Hg","Platelets","INR","PT","PTT","WBC",\
                "RBC","TEMP","ArtBE","ArtCO2", "ArtPaCO2","ArtPaO2","ArtpH","FiO2Set","PEEPSet","RespTot","RespSet","RespSpon",\
                "PIP","PlateauPres","TidVolObs","TidVolSet","TidVolSpon","SaO2","","Pacemaker","IABP_Setting","ServiceType"]
        
        #4*4
        set2 = [ "NBPSys", "NBPDias", "NBPMean", "NBP", "SBP", "DBP", "MAP", "HR", "RESP", "SpO2", "CVP", "PAPMean", "PAPSd",\
                 "urineout", "totalout","imputrbcs", "inputotherblood", "totalinput"]
        
        #10*4
        set3 = [ "CRDIndx", "SVR", "COtd", "COfck", "PCWP", "PVR", "Trach", "SkinColor", "SkinIntegrity"]
        
        #3*4
        set4 = [ "Heart_Rhythm", "Ectopy_Type", "Ectopy_Freq", "Code_Status", "FallRisk"]
        
        #5*4
        set5 = [ "Orientation", "RikerSAS", "Vent", "VentMode"]

        set1 =[feat.lower() for feat in set1]
        set2 =[feat.lower() for feat in set2]
        set3 =[feat.lower() for feat in set3]
        set4 =[feat.lower() for feat in set4]
        set5 =[feat.lower() for feat in set5]

        for i in range(0,len(attributes)):
                #print attributes[i]
                if attributes[i] in set1 :
                        [attributes, patient_measurements] = self.perform_hold_approach_per_attribute(attributes, patient_measurements, i, 28*4)
                elif attributes[i] in set2:
                        [attributes, patient_measurements] = self.perform_hold_approach_per_attribute(attributes, patient_measurements, i, 4*4)  
                elif attributes[i] in set3:
                        [attributes, patient_measurements] = self.perform_hold_approach_per_attribute(attributes, patient_measurements, i, 10*4) 
                elif attributes[i] in set4:
                        [attributes, patient_measurements] = self.perform_hold_approach_per_attribute(attributes, patient_measurements, i, 3*4) 
                elif attributes[i] in set5:
                        [attributes, patient_measurements] = self.perform_hold_approach_per_attribute(attributes, patient_measurements, i, 5*4)         
        
        return [attributes, patient_measurements]
    
    def perform_hold_approach_per_attribute(self, attributes, patient_measurements, attribute, max_hold_time):
        counter = 1
        most_recent_value =  patient_measurements[0,attribute].strip()
        #print most_recent_value
        for i in range(1, len(patient_measurements)):
            if patient_measurements[i,attribute].strip() is not '' or patient_measurements[i,attribute].strip() is not 0 :
                most_recent_value = patient_measurements[i,attribute]
                counter = 0
            elif counter < max_hold_time and most_recent_value is not '' or patient_measurements[i,attribute].strip() is not 0:
                patient_measurements[i,attribute] = most_recent_value
                
            counter += 1
            
        return [attributes, patient_measurements]
    
    # Retun a list of filtered patient Id's
    def determine_complete_patient_ids(self,File):
        rows = io.read_csv(File, ',');
        current_ID  = 0
        previous_ID = 0
        complete_patient_ID = []
                
        counter = 0
        BUN= False
        GCS= False
        HR = False
        Hem= False
        IV = False

        for row in rows:
            if counter == 0:
                counter +=1
            else:
                current_ID  = row[0]
                if float(GCS) == 1.0 and float(BUN) == 1.0 and float(HR) == 1.0 and float(Hem) == 1.0 and float(IV) == 1.0: 
                    if counter > 0 and not previous_ID == current_ID :
                        complete_patient_ID.append(previous_ID)
                        #print previous_ID
                        #print len(complete_patient_ID)
                        BUN = False
                        GCS = False
                        Hem = False
                        HR  = False
                        IV  = False
                
                previous_ID = current_ID
                counter +=1

                if not row[2].strip() is '':
                    if float(row[2])  !=  0:
                        GCS = True
                if not row[13].strip() is '':
                    if float(row[13]) !=  0 :
                        HR  = True
                if not row[30].strip() is '':
                    if float(row[30]) !=  0 :
                        BUN = True
                if not row[43].strip() is '':
                    if float(row[43]) != 0 :
                        Hem = True
                if not IV == True:
                    for i in range(86,135):
                        if not row[i].strip() is '':
                            if float(row[i]) != 0:
                                IV=True
                                break
#         print complete_patient_ID
#         print len(complete_patient_ID)
        return  complete_patient_ID
    
    
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
        [attributes, patient_measurements] = self.perform_hold_approach(attributes,patient_measurements)
        [attributes_extended, patient_measurements] = self.derived_variables(attributes, patient_measurements)
        # medication scaling
        [attributes_extended,patient_measurements] = self.medication_scaling(attributes_extended, patient_measurements)
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
        return final_attributes, np.array(final_values)

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
        
        #perform 'hold' appraoch
        [attributes, patient_measurements] = self.perform_hold_approach(attributes,patient_measurements)
        
        #calculate derived variables
        [attributes_extended, patient_measurements] = self.derived_variables(copy.deepcopy(attributes), patient_measurements)
        
        #medication scaling
        [attributes_extended,patient_measurements] = self.medication_scaling(copy.deepcopy(attributes_extended), patient_measurements)

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
            [attr, values] = self.aggregate_day_sdas_das(attributes, patient_measurements, day_count, type)
            return [attr, values.astype(np.float64, copy=False)]
        elif type == 2:
            [attr, values] = self.aggregate_day_knn(attributes, patient_measurements, day_count)
            return [attr, values.astype(np.float64, copy=False)]

    # Aggregation for both the SDAS and DAS approach, if more fine-grained predictions are
    # required this should slightly change.

    def aggregate(self, type):

        # The numpy data structure in which all aggregated data will be stored, the day will replace the time
        # The ID will also be included.
        aggr_set = np.zeros((0, 0))
        new_attributes = []
        count = 0
        row_count = 0

        for ID in self.patient_dict:
            if (count % 1000 == 0):
                print '====== ' + str(count)
            count += 1
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
                        self.writer.writerow(attr)
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
                        #edit by Ali (for Windows)
                        current_values.append(((datetime.datetime(*self.patient_dict[ID][self.headers[i]][index][:7])- datetime.datetime(1900,1,1)).total_seconds()))
                        #current_values.append(time.mktime(self.patient_dict[ID][self.headers[i]][index]))
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

            for r in range(row_count, aggr_set.shape[0]):
                self.writer.writerow(aggr_set[r,:])
            row_count = aggr_set.shape[0]

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
    
    def read_files_and_calculate_attributes(self, file, file_out, type=0):

        self.writer = io.write_csv(file_out)
        complete_patient_ids = self.determine_complete_patient_ids(file)
        len(complete_patient_ids)
        print '====== reading the data'
        rows = io.read_csv(file, ',');
        print '====== pointer to data obtained'

        counter = 0
        ids = []
        dataset_headers = []

        while counter < 3500000:
        #for row in rows:
                row=rows.next()

                if counter % 10000 == 0:
                    print '====== ' + str(counter)
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
                elif  row[0] in complete_patient_ids:
                    # Assuming ID is the first attribute.
                    id = row[0]
                    if id not in ids :
                        ids.append(id)
                        self.patient_dict[id] = {}
                        for header in self.headers:
                            self.patient_dict[id][header] = []
                    
                    # Get the time to order based upon it
                    # Time had this structure in original csv: 07-SEP-82 07.30.00.000000000 PM US/EASTERN 
                    # Time had this structure now :2682-09-07 18:3
                    #this was changed by Ali because of the new dataset
                    timestamp = time.strptime(row[self.headers.index('charttime')+1][0:16],"%Y-%m-%d %H:%M") 
                    timestamp = time.strptime(time.strftime('%d-%b-%y %H.%M', timestamp),'%d-%b-%y %H.%M', )
                    
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
                            [features, values] = self.process_value_individual(dataset_headers[row_index-1], row[row_index], type)
                            for i in range(0, len(values)):
                                self.patient_dict[id][features[i]].insert(index, values[i])
                    # Now assign the label
                    self.patient_dict[id]['label'].insert(index, self.determine_class(self.patient_dict[id]['daysfromdischtodeath'][index], self.patient_dict[id]['icustay_expire_flg'][index]))
                counter += 1
        print self.patient_dict.keys()
        print len(self.patient_dict.keys())
        return self.aggregate_data(type)
