# Given A Pandas Dataframe of MIMIC II Data, perform the same queries as in SQL. 
# Usage: Please query from cross_query_MIMIC, not the individual queries.

import pandas as pd, numpy as np
import pickle, os, sys, datetime
import mimic_process_queries as process

# Note : 
# ------
# For almost all of the queries, I still merge on ICUSTAY_ID b/c certain 
# chartevents are specific to an ICUSTAY_ID. Even though we only care 
# about SUBJECTS, we should perserve the difference until later!

# ------------------------  PART 1  ------------------------
# Because of resource constraints, the database that 
# we are querying from is split into several Pandas structures. Therefore,
# although the queries are written as if they are querying a single DB,
# we must define a wrapper function to query and combine from all partitions.

# query fxn - is any function found below
# query args - any addition arguments needed for the query 
#			   does NOT contain the mimic_db itself. 
# mimic_dbs - an array of pandas partitions
def cross_query_MIMIC(mimic_dbs, queryfxn, *args):
	# Make the query call for each partition
	query_results = []
	for mimic_db in mimic_dbs:
		result = queryfxn(mimic_db, *args)
		query_results.append(result)
	# Now we need to combine them all
	combined = pd.concat(query_results)
	# Fix the indicies 
	combined.set_index([np.arange(1, combined.shape[0] + 1)])
	return combined

# In the case that I cannot load all of these at once... then
def iterative_query_MIMIC(mimic_pkls, queryfxn, queryname, *args):
	print 'Performing query function %s on all partitions...' % queryname
	for mimic_pkl in mimic_pkls:
		print '---------------------------'
		print 'Loading pickle file %s...' % mimic_pkl 
		# Load the pickle file in the path
		mimic_db = pd.read_pickle(mimic_pkl)
		print 'Performing the query ...'
		result = queryfxn(mimic_db, *args)
		print 'Saving result to file ...'
		# Save the individual result pieces as pickles
		save_result = os.path.join('/n/dtak/mimic-ii/query-data/', queryname + '-' + mimic_pkl[-6:])
		result.to_pickle(save_result)
		print '---------------------------' 

# ------------------------  PART 2  ------------------------
# This section defines general queries to a single pandas structure. 

# *** Section 1 - Defining Populations *** 

# We start by considering only the first ICU admission of each hospital 
# admission. Use this for create_covariates_table.
def create_table_POP1(mimic_db):
	# Perform a left outer join
	POP1 = mimic_db['ADMISSIONS'].merge(mimic_db['ICUSTAY_DETAIL'], how='left', on='HADM_ID')
	# Perform 'where' statement
	POP1 = POP1[POP1['ICUSTAY_FIRST_FLG'] == 'Y']
	# Pick out 5 columns
	POP1 = POP1[['HADM_ID', 'SUBJECT_ID_x', 'ICUSTAY_ID', 'HOSPITAL_ADMIT_DT', 'ICUSTAY_INTIME']]
	# Rename the 5 columns
	POP1.columns = ['HADM_ID', 'SUBJECT_ID', 'ICUSTAY_ID', 'HOSPITAL_ADMIT_DT', 'ICUSTAY_INTIME'] 
	return POP1

# Use this for vital signs + lab results
def create_table_POP(mimic_db):
	# Limit the population
	POP = mimic_db['ICUSTAY_DETAIL']
	# Criterions 1: first time in ICU, Adult patient, between 12 - 96 hours of ICU stay
	POP = POP[(POP['ICUSTAY_SEQ'] == 1) & (POP['ICUSTAY_AGE_GROUP'] == 'adult') & (POP['ICUSTAY_LOS'] >= 12*60) & (POP['ICUSTAY_LOS'] <= 96*60)]
	# Criterion 2: 1) Exclude CMO, 2) Exclude DNR/DNI, 3) Include only Full Code, 4) No NSICU, CSICU 
	# Merge the patient data with chartevents
	MERGED = POP.merge(mimic_db['CHARTEVENTS'], on='ICUSTAY_ID', how='left')
	# Find PACEMAKER data, Find RISK FOR FALLS data
	PACEMAKER = MERGED[MERGED['ITEMID'] == 1484][['ICUSTAY_ID', 'VALUE1']]
	RISKFALLS = MERGED[MERGED['ITEMID'] == 516][['ICUSTAY_ID', 'VALUE1']]
	PACEMAKER = PACEMAKER.groupby('ICUSTAY_ID', as_index=False).agg(lambda x: x.iloc[0])
	RISKFALLS = RISKFALLS.groupby('ICUSTAY_ID', as_index=False).agg(lambda x: x.iloc[0])
	PACEMAKER.rename(columns={'VALUE1': 'PACEMAKER'}, inplace=True)
	RISKFALLS.rename(columns={'VALUE1': 'RISKFALLS'}, inplace=True)

	all_ICUSTAY_ID = pd.unique(MERGED['ICUSTAY_ID'].values.ravel())
	# Grab out only the events WITHOUT full code for care protocol
	MERGED = MERGED[(MERGED['ITEMID'] == 128) & (MERGED['VALUE1'] != 'Full Code')]
	bad_ICUSTAY_ID = pd.unique(MERGED['ICUSTAY_ID'].values.ravel())
	# Subtract the two sets 
	good_ICUSTAY_ID = np.array([i for i in all_ICUSTAY_ID if i not in bad_ICUSTAY_ID])
	POP = POP[POP['ICUSTAY_ID'].isin(good_ICUSTAY_ID)]
	# Remove any NSICU Service or CSICU Service patients
	POP = POP[~POP['ICUSTAY_FIRST_SERVICE'].isin(['NSICU', 'CSICU'])]
	
	# Merge with the selection data
	POP = POP.merge(PACEMAKER, on='ICUSTAY_ID', how='left')
	POP['PACEMAKER'].fillna('No', inplace=True)
	POP = POP.merge(RISKFALLS, on='ICUSTAY_ID', how='left')
	POP['RISKFALLS'].fillna('None', inplace=True)
	return POP 

# ----------------------------------------------------------------------------------------

# *** Section 2 - Extract Baseline Covariates *** 
# ------------------- TAKES THE ENTIRE STREAM OF DATA AFTER THE ICUSTAY TIME :) ----------------------

def create_demographics_tables(POP):
	# Do some extra constraining on the POP set
	DEMOGRAPHIC = POP[POP['SUBJECT_ICUSTAY_SEQ'] == 1]
	DEMOGRAPHIC = DEMOGRAPHIC[['SUBJECT_ID', 'ICUSTAY_ID', 'ICUSTAY_INTIME', 'ICUSTAY_OUTTIME', 'ICUSTAY_ADMIT_AGE', 'GENDER', 'SAPSI_FIRST', 'WEIGHT_FIRST', 'SOFA_FIRST', 'ICUSTAY_FIRST_SERVICE', 'PACEMAKER', 'RISKFALLS']]
	# Change the columns to be categories, not strings
	DEMOGRAPHIC['ICUSTAY_FIRST_SERVICE'] = process.discretize_categories(DEMOGRAPHIC['ICUSTAY_FIRST_SERVICE'])
	DEMOGRAPHIC['GENDER'] = process.discretize_categories(DEMOGRAPHIC['GENDER'])
	DEMOGRAPHIC['PACEMAKER'] = process.discretize_categories(DEMOGRAPHIC['PACEMAKER'])
	DEMOGRAPHIC['RISKFALLS'] = process.discretize_categories(DEMOGRAPHIC['RISKFALLS'])
	# Add BMI to the table
	DEMOGRAPHIC['BMI'] = POP['WEIGHT_FIRST'] / (POP['HEIGHT'] * POP['HEIGHT'])
	return DEMOGRAPHIC

# Duplicate of demographic data but with OUTCOMES parameters
def create_outcomes_tables(POP):
	OUTCOMES = POP[POP['SUBJECT_ICUSTAY_SEQ'] == 1]
	OUTCOMES = OUTCOMES[['SUBJECT_ID', 'ICUSTAY_ID', 'ICUSTAY_INTIME', 'ICUSTAY_OUTTIME', 'HOSPITAL_EXPIRE_FLG', 'DOD', 'ICUSTAY_ADMIT_AGE', 'GENDER', 'SAPSI_FIRST', 'WEIGHT_FIRST', 'SOFA_FIRST', 'ICUSTAY_FIRST_SERVICE', 'PACEMAKER', 'RISKFALLS']]

	# Conservative assumption : if no data, we assume they didn't die.
	OUTCOMES['HOSPITAL_EXPIRE_FLG'].fillna('N', inplace=True)
	# For Hospital expire flag, we need to replace the values by hand because we care what order.
	OUTCOMES['HOSPITAL_EXPIRE_FLG'].replace('N', 0, inplace=True)
	OUTCOMES['HOSPITAL_EXPIRE_FLG'].replace('Y', 1, inplace=True)

	# Change the columns to be categories, not strings
	OUTCOMES['ICUSTAY_FIRST_SERVICE'] = process.discretize_categories(OUTCOMES['ICUSTAY_FIRST_SERVICE'])
	OUTCOMES['GENDER'] = process.discretize_categories(OUTCOMES['GENDER'])
	OUTCOMES['PACEMAKER'] = process.discretize_categories(OUTCOMES['PACEMAKER'])
	OUTCOMES['RISKFALLS'] = process.discretize_categories(OUTCOMES['RISKFALLS'])
	# Add BMI to the table
	OUTCOMES['BMI'] = POP['WEIGHT_FIRST'] / (POP['HEIGHT'] * POP['HEIGHT'])
	return OUTCOMES

# The following functions denote Vital Signs
def create_vitals_tables(mimic_db, POP):
	VAR = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='ICUSTAY_ID')
	VAR.rename(columns={'SUBJECT_ID_x' : 'SUBJECT_ID'}, inplace=True)

	VAR2 = POP.merge(mimic_db['IOEVENTS'], how='left', on='ICUSTAY_ID')
	VAR2.rename(columns={'SUBJECT_ID_x' : 'SUBJECT_ID'}, inplace=True)

	# Make all the queries
	VAR_MEAN_BP = VAR[(VAR['ITEMID'] == 52) | (VAR['ITEMID'] == 456)]
	VAR_MEAN_BP = VAR_MEAN_BP[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	VAR_MEAN_BP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	print("Completed Mean BP")

	VAR_TEMP = VAR[(VAR['ITEMID'] == 678) | (VAR['ITEMID'] == 679)]
	VAR_TEMP = VAR_TEMP[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	VAR_TEMP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	print("Completed TEMP")

	VAR_HR = VAR[VAR['ITEMID'] == 211]
	VAR_HR = VAR_HR[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	VAR_HR.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	print("Completed HR")

	# VAR_CVP = VAR[VAR['ITEMID'] == 113]
	# VAR_CVP = VAR_CVP[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	# VAR_CVP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	# print("Completed CVP")

	VAR_SPO2 = VAR[VAR['ITEMID'] == 646]
	VAR_SPO2 = VAR_SPO2[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	VAR_SPO2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	print("Completed SPO2")

	VAR_FIO2 = VAR[(VAR['ITEMID'] == 190) | (VAR['ITEMID'] == 3420)]
	VAR_FIO2 = VAR_FIO2[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	VAR_FIO2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	print("Completed FIO2")

	# VAR_GCS = VAR[VAR['ITEMID'] == 198]
	# VAR_GCS = VAR_GCS[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	# VAR_GCS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	# print("Completed GCS")

	# VAR_CARE_PROTOCOL = VAR[VAR['ITEMID'] == 128]
	# VAR_CARE_PROTOCOL = VAR_CARE_PROTOCOL[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	# VAR_CARE_PROTOCOL.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	# print("Completed CARE_PROTOCOL")

	# VAR_WEIGHT = VAR[VAR['ITEMID'] == 3580]
	# VAR_WEIGHT = VAR_WEIGHT[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	# VAR_WEIGHT.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	# print("Completed WEIGHT")

	# VAR_VENTILATED_RR = VAR[VAR['ITEMID'] == 619]
	# VAR_VENTILATED_RR = VAR_VENTILATED_RR[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	# VAR_VENTILATED_RR.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	# print("Completed VENTILATED_RR")

	VAR_SPONTANEOUS_RR = VAR[(VAR['ITEMID'] == 614) | (VAR['ITEMID'] == 615) | (VAR['ITEMID'] == 618)]
	VAR_SPONTANEOUS_RR = VAR_SPONTANEOUS_RR[(VAR_SPONTANEOUS_RR['VALUE1NUM'] >= 2) | (VAR_SPONTANEOUS_RR['VALUE1NUM'] <= 80)]
	VAR_SPONTANEOUS_RR = VAR_SPONTANEOUS_RR[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE1NUM']]
	VAR_SPONTANEOUS_RR.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
	print("Completed SPONTANEOUS_RR")

	# Query the Urine output volume
	VAR_URINE = VAR2[VAR2['ITEMID'].isin([55, 56, 57, 61, 65, 69, 85, 94, 96, 288, 405,428, 473, 651, 715, 1922, 2042, 2068, 2111, 2119, 2130, 2366, 2463,2507, 2510, 2592, 2676, 2810, 2859, 3053, 3175, 3462, 3519, 3966, 3987, 4132, 4253, 5927])]
	VAR_URINE = VAR_URINE[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VOLUME']]
	VAR_URINE.rename(columns={'VOLUME' : 'VALUE'}, inplace=True)
	print("Completed URINE")

	return VAR_MEAN_BP, VAR_TEMP, VAR_HR, VAR_SPO2, VAR_FIO2, VAR_SPONTANEOUS_RR, VAR_URINE


def create_lab_results_tables(mimic_db, POP):
	VAR = POP.merge(mimic_db['LABEVENTS'], on='ICUSTAY_ID', how='left')
	VAR.rename(columns={'SUBJECT_ID_x' : 'SUBJECT_ID'}, inplace=True)

	# Make all the queries
	# VAR_ALBUMIN = VAR[VAR['ITEMID'] == 50060]
	# VAR_ALBUMIN = VAR_ALBUMIN[['SUBJECT_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_ALBUMIN.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed ALBUMIN")

	# VAR_ALP = VAR[VAR['ITEMID'] == 50061]
	# VAR_ALP = VAR_ALP[['SUBJECT_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_ALP.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed ALP")

	# VAR_ALT = VAR[VAR['ITEMID'] == 50062]
	# VAR_ALT = VAR_ALT[['SUBJECT_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_ALT.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed ALT")

	# VAR_AST = VAR[VAR['ITEMID'] == 50073]
	# VAR_AST = VAR_AST[['SUBJECT_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_AST.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed AST")

	# VAR_BILIRUBIN = VAR[VAR['ITEMID'] == 50626]
	# VAR_BILIRUBIN = VAR_BILIRUBIN[['SUBJECT_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_BILIRUBIN.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed BILIRUBIN")

	VAR_BUN = VAR[VAR['ITEMID'] == 50177]
	VAR_BUN = VAR_BUN[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_BUN.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed BUN")

	# VAR_CHOLESTEROL = VAR[VAR['ITEMID'] == 50085]
	# VAR_CHOLESTEROL = VAR_CHOLESTEROL[['SUBJECT_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_CHOLESTEROL.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed CHOLESTEROL")

	VAR_CREATININE = VAR[VAR['ITEMID'] == 50090]
	VAR_CREATININE = VAR_CREATININE[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_CREATININE.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed CREATININE")

	VAR_GLUCOSE = VAR[(VAR['ITEMID'] == 50006) | (VAR['ITEMID'] == 50112) ]
	VAR_GLUCOSE = VAR_GLUCOSE[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_GLUCOSE.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed GLUCOSE")

	VAR_BICARBONATE = VAR[(VAR['ITEMID'] == 50172) | (VAR['ITEMID'] == 50025) | (VAR['ITEMID'] == 50022)]
	VAR_BICARBONATE = VAR_BICARBONATE[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_BICARBONATE.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed BICARBONATE")

	VAR_HCT = VAR[(VAR['ITEMID'] == 50029) | (VAR['ITEMID'] == 50383) ]
	VAR_HCT = VAR_HCT[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_HCT.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed HCT")

	VAR_LACTATE = VAR[VAR['ITEMID'] == 50010]
	VAR_LACTATE = VAR_LACTATE[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_LACTATE.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed LACTATE")

	VAR_MAGNESIUM = VAR[VAR['ITEMID'] == 50140]
	VAR_MAGNESIUM = VAR_MAGNESIUM[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_MAGNESIUM.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed MAGNESIUM")

	# VAR_PACO2 = VAR[VAR['ITEMID'] == 50016]
	# VAR_PACO2 = VAR_PACO2[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_PACO2.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed PACO2")

	# VAR_PH = VAR[VAR['ITEMID'] == 50018]
	# VAR_PH = VAR_PH[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_PH.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed PH")

	VAR_PLATELETS = VAR[VAR['ITEMID'] == 50428]
	VAR_PLATELETS = VAR_PLATELETS[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_PLATELETS.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed PLATELETS")

	# VAR_PO2 = VAR[VAR['ITEMID'] == 50019]
	# VAR_PO2 = VAR_PO2[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_PO2.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed PO2")

	VAR_POTASSIUM = VAR[(VAR['ITEMID'] == 50149) | (VAR['ITEMID'] == 50009)]
	VAR_POTASSIUM = VAR_POTASSIUM[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_POTASSIUM.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed POTASSIUM")

	# VAR_SAO2 = VAR[VAR['ITEMID'] == 50015]
	# VAR_SAO2 = VAR_SAO2[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_SAO2.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed SAO2")

	VAR_SODIUM = VAR[(VAR['ITEMID'] == 50159) | (VAR['ITEMID'] == 50012)]
	VAR_SODIUM = VAR_SODIUM[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_SODIUM.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed SODIUM")

	# Dropping INR for now b/c it is so sparse (actually empty)
	# VAR_INR = VAR[(VAR['ITEMID'] == 815) | (VAR['ITEMID'] == 1530)]
	# VAR_INR = VAR_INR[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_INR.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed INR")

	# VAR_TROPI = VAR[VAR['ITEMID'] == 50188]
	# VAR_TROPI = VAR_TROPI[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_TROPI.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed TROPI")

	# VAR_TROPT = VAR[VAR['ITEMID'] == 50189]
	# VAR_TROPT = VAR_TROPT[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	# VAR_TROPT.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	# print("Completed TROPT")

	VAR_WBC = VAR[(VAR['ITEMID'] == 50316) | (VAR['ITEMID'] == 50468)]
	VAR_WBC = VAR_WBC[['SUBJECT_ID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUENUM']]
	VAR_WBC.rename(columns={'VALUENUM' : 'VALUE'}, inplace=True)
	print("Completed WBC")

	return VAR_BUN, VAR_CREATININE, VAR_GLUCOSE, VAR_BICARBONATE, VAR_HCT, \
		VAR_LACTATE, VAR_MAGNESIUM, VAR_PLATELETS, VAR_POTASSIUM, VAR_SODIUM, \
		VAR_WBC


# The following function is for querying ALL of the covariates at once (faster)
# ------------------ DEPRECATED ------------------
# def create_covariates_tables(mimic_db, POP):
# 	VAR = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID')
# 	# Only consider those above patient icustay times! 
# 	VAR = VAR[VAR['CHARTTIME'] - VAR['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)]
# 	# Here we can make all the queries!
# 	VAR_TEMP = VAR[(VAR['ITEMID'] == 678) | (VAR['ITEMID'] == 679)]
# 	VAR_TEMP = VAR_TEMP[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_TEMP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed TEMP")

# 	VAR_RR2 = VAR[VAR['ITEMID'] == 618]
# 	VAR_RR2 = VAR_RR2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_RR2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed RR2")

# 	VAR_BP2 = VAR[VAR['ITEMID'] == 51]
# 	VAR_SYS_BP2 = VAR_BP2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_SYS_BP2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	VAR_DIA_BP2 = VAR_BP2[['SUBJECT_ID', 'CHARTTIME', 'VALUE2NUM']]
# 	VAR_DIA_BP2.rename(columns={'VALUE2NUM' : 'VALUE'}, inplace=True)
# 	print("Completed BPs")

# 	VAR_WBC2 = VAR[(VAR['ITEMID'] == 1542) | (VAR['ITEMID'] == 1127) | (VAR['ITEMID'] == 861) | (VAR['ITEMID'] == 4200)]
# 	VAR_WBC2 = VAR_WBC2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_WBC2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)	
# 	print("Completed WBC2")

# 	VAR_HR2 = VAR[VAR['ITEMID'] == 221]
# 	VAR_HR2 = VAR_HR2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_HR2.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_HR2['VALUE'] = VAR_HR2['VALUE'].apply(HR_convert_to_value)
# 	print("Completed HR2")

# 	VAR_VENT2 = VAR[VAR['ITEMID'] == 772]
# 	VAR_VENT2 = VAR_VENT2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_VENT2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed VENT2")

# 	VAR_CVP = VAR[VAR['ITEMID'] == 113]
# 	VAR_CVP = VAR_CVP[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_CVP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed CVP")

# 	VAR_LACTATE2 = VAR[(VAR['ITEMID'] == 818) | (VAR['ITEMID'] == 1531)]
# 	VAR_LACTATE2 = VAR_LACTATE2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_LACTATE2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)	
# 	print("Completed LACTATE2")

# 	VAR_CODE_STATUS = VAR[VAR['ITEMID'] == 128]
# 	VAR_CODE_STATUS = VAR_CODE_STATUS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_CODE_STATUS.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_CODE_STATUS['VALUE'] = discretize_categories(VAR_CODE_STATUS['VALUE'])
# 	print("Completed CODE_STATUS")

# 	VAR_PRECAUTIONS = VAR[VAR['ITEMID'] == 1550]
# 	VAR_PRECAUTIONS = VAR_PRECAUTIONS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_PRECAUTIONS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	VAR_PRECAUTIONS['VALUE'] = discretize_categories(VAR_PRECAUTIONS['VALUE'])
# 	print("Completed PRECAUTIONS")	

# 	VAR_FALLS = VAR[VAR['ITEMID'] == 1484]
# 	VAR_FALLS = VAR_FALLS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_FALLS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)	
# 	VAR_FALLS['VALUE'] = VAR_FALLS['VALUE'].fillna(0) # Binary!	
# 	print("Completed FALLS")

# 	VAR_RIKER_SAS = VAR[VAR['ITEMID'] == 1337]
# 	VAR_RIKER_SAS = VAR_RIKER_SAS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_RIKER_SAS.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_RIKER_SAS['VALUE'] = discretize_categories(VAR_RIKER_SAS['VALUE'])
# 	print("Completed RIKER_SAS")

# 	VAR_HEALTHCARE_PROXY = VAR[VAR['ITEMID'] == 1703]
# 	VAR_HEALTHCARE_PROXY = VAR_HEALTHCARE_PROXY[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_HEALTHCARE_PROXY.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_HEALTHCARE_PROXY['VALUE'] = discretize_categories(VAR_HEALTHCARE_PROXY['VALUE'])
# 	print("Completed HEALTHCARE_PROXY")

# 	VAR_BED = VAR[VAR['ITEMID'] == 680]
# 	VAR_BED = VAR_BED[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_BED.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_BED['VALUE'] = discretize_categories(VAR_BED['VALUE'])
# 	print("Completed BED")

# 	VAR_SKIN_INTEGRITY = VAR[VAR['ITEMID'] == 644]
# 	VAR_SKIN_INTEGRITY = VAR_SKIN_INTEGRITY[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_SKIN_INTEGRITY.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_SKIN_INTEGRITY['VALUE'] = discretize_categories(VAR_SKIN_INTEGRITY['VALUE'])
# 	print("Completed SKIN_INTEGRITY")

# 	VAR_GIPROPHYLAXIS = VAR[VAR['ITEMID'] == 1427]
# 	VAR_GIPROPHYLAXIS = VAR_GIPROPHYLAXIS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_GIPROPHYLAXIS.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_GIPROPHYLAXIS['VALUE'] = discretize_categories(VAR_GIPROPHYLAXIS['VALUE'])
# 	print("Completed GIPROPHYLAXIS")

# 	VAR_HEMOGLOBIN = VAR[VAR['ITEMID'] == 814]
# 	VAR_HEMOGLOBIN = VAR_HEMOGLOBIN[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_HEMOGLOBIN.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed HEMOGLOBIN")

# 	VAR_PLATELETS = VAR[VAR['ITEMID'] == 828]
# 	VAR_PLATELETS = VAR_PLATELETS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_PLATELETS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed PLATELETS")

# 	VAR_CPK = VAR[VAR['ITEMID'] == 784]
# 	VAR_CPK = VAR_CPK[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_CPK.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed CPK")

# 	VAR_GUAIC = VAR[VAR['ITEMID'] == 660]
# 	VAR_GUAIC = VAR_GUAIC[['SUBJECT_ID', 'CHARTTIME', 'VALUE1']]
# 	VAR_GUAIC.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	VAR_GUAIC['VALUE'] = discretize_categories(VAR_GUAIC['VALUE'])
# 	print("Completed GUAIC")

# 	VAR_AMYLASE = VAR[VAR['ITEMID'] == 775]
# 	VAR_AMYLASE = VAR_AMYLASE[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_AMYLASE.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	print("Completed AMYLASE")

# 	return VAR_TEMP, VAR_RR2, VAR_SYS_BP2, VAR_DIA_BP2, VAR_WBC2, VAR_HR2, \
# 		VAR_VENT2, VAR_CVP, VAR_LACTATE2, VAR_CODE_STATUS, VAR_PRECAUTIONS, \
# 		VAR_FALLS, VAR_RIKER_SAS, VAR_HEALTHCARE_PROXY, VAR_BED, VAR_SKIN_INTEGRITY, \
# 		VAR_GIPROPHYLAXIS, VAR_HEMOGLOBIN, VAR_PLATELETS, VAR_CPK, VAR_GUAIC, VAR_AMYLASE

# ----------------------------------------------------------------------------------------

# The following functions are for creating SEPARATE queries (slower, but good if you only need one)
# def create_table_VAR_TEMP(mimic_db, POP): 
# 	VAR_TEMP = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID')
# 	VAR_TEMP = VAR_TEMP[(VAR_TEMP['CHARTTIME'] - VAR_TEMP['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 		((VAR_TEMP['ITEMID'] == 678) | (VAR_TEMP['ITEMID'] == 679))]
# 	VAR_TEMP = VAR_TEMP[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_TEMP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_TEMP

# def create_table_VAR_RR2(mimic_db, POP):
# 	VAR_RR2 = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID')
# 	VAR_RR2 = VAR_RR2[(VAR_RR2['CHARTTIME'] - VAR_RR2['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_RR2['ITEMID'] == 618)]
# 	VAR_RR2 = VAR_RR2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_RR2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_RR2

# def create_table_VAR_BP2(mimic_db, POP):
# 	VAR_BP2 = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID')
# 	VAR_BP2 = VAR_BP2[(VAR_BP2['CHARTTIME'] - VAR_BP2['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_BP2['ITEMID'] == 51)]
# 	VAR_BP2 = VAR_BP2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM', 'VALUE2NUM']]
# 	VAR_BP2.rename(columns={'VALUE1NUM' : 'SYS_VALUE', 'VALUE2NUM' : 'DIA_VALUE'}, inplace=True)
# 	return VAR_BP2

# def create_table_VAR_WBC2(mimic_db, POP):
# 	VAR_WBC2 = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID')
# 	VAR_WBC2 = VAR_WBC2[(VAR_WBC2['CHARTTIME'] - VAR_WBC2['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	((VAR_WBC2['ITEMID'] == 1542) |  (VAR_WBC2['ITEMID'] == 1127) | (VAR_WBC2['ITEMID'] == 861) | (VAR_WBC2['ITEMID'] == 4200))]
# 	VAR_WBC2 = VAR_WBC2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_WBC2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_WBC2

# def create_table_VAR_HR2(mimic_db, POP):
# 	VAR_HR2 = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID')
# 	VAR_HR2 = VAR_HR2[(VAR_HR2['CHARTTIME'] - VAR_HR2['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_HR2['ITEMID'] == 221)]
# 	VAR_HR2 = VAR_HR2[['SUBJECT_ID','CHARTTIME', 'VALUE1']]
# 	VAR_HR2.rename(columns={'VALUE1' : 'VALUE'}, inplace=True)
# 	# Convert it all to floats! (with error handling)
# 	VAR_HR2['VALUE'] = VAR_HR2['VALUE'].apply(HR_convert_to_value)
# 	return VAR_HR2

# def create_table_VAR_VENT2(mimic_db, POP):
# 	VAR_VENT2 = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_VENT2 = VAR_VENT2[(VAR_VENT2['CHARTTIME'] - VAR_VENT2['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_VENT2['ITEMID'] == 772)]
# 	VAR_VENT2 = VAR_VENT2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_VENT2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_VENT2

# def create_table_VAR_CVP(mimic_db, POP):
# 	VAR_CVP = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_CVP = VAR_CVP[(VAR_CVP['CHARTTIME'] - VAR_CVP['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_CVP['ITEMID'] == 113)]
# 	VAR_CVP = VAR_CVP[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_CVP.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_CVP

# # Note: skipping mental_status because it contains strings. 
# def create_table_VAR_LACTATE2(mimic_db, POP):
# 	VAR_LACTATE2 = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_LACTATE2 = VAR_LACTATE2[(VAR_LACTATE2['CHARTTIME'] - VAR_LACTATE2['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	((VAR_LACTATE2['ITEMID'] == 818) | (VAR_LACTATE2['ITEMID'] == 1531))]
# 	VAR_LACTATE2 = VAR_LACTATE2[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_LACTATE2.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_LACTATE2

# def create_table_VAR_CODE_STATUS(mimic_db, POP):
# 	VAR_CODE_STATUS = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_CODE_STATUS = VAR_CODE_STATUS[(VAR_CODE_STATUS['CHARTTIME'] - VAR_CODE_STATUS['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_CODE_STATUS['ITEMID'] == 128)]
# 	VAR_CODE_STATUS = VAR_CODE_STATUS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_CODE_STATUS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_CODE_STATUS

# def create_table_VAR_PRECAUTIONS(mimic_db, POP):
# 	VAR_PRECAUTIONS = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_PRECAUTIONS = VAR_PRECAUTIONS[(VAR_PRECAUTIONS['CHARTTIME'] - VAR_PRECAUTIONS['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_PRECAUTIONS['ITEMID'] == 1550)]
# 	VAR_PRECAUTIONS = VAR_PRECAUTIONS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_PRECAUTIONS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_PRECAUTIONS

# def create_table_VAR_FALLS(mimic_db, POP):
# 	VAR_FALLS = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_FALLS = VAR_FALLS[(VAR_FALLS['CHARTTIME'] - VAR_FALLS['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_FALLS['ITEMID'] == 1484)]
# 	VAR_FALLS = VAR_FALLS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_FALLS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	VAR_FALLS['VALUE'] = VAR_FALLS['VALUE'].fillna(0) # Binary!
# 	return VAR_FALLS

# def create_table_VAR_RIKER_SAS(mimic_db, POP):
# 	VAR_RIKER_SAS = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_RIKER_SAS = VAR_RIKER_SAS[(VAR_RIKER_SAS['CHARTTIME'] - VAR_RIKER_SAS['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_RIKER_SAS['ITEMID'] == 1337)]
# 	VAR_RIKER_SAS = VAR_RIKER_SAS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_RIKER_SAS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_RIKER_SAS

# def create_table_VAR_HEALTHCARE_PROXY(mimic_db, POP):
# 	VAR_HEALTHCARE_PROXY = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_HEALTHCARE_PROXY = VAR_HEALTHCARE_PROXY[(VAR_HEALTHCARE_PROXY['CHARTTIME'] - VAR_HEALTHCARE_PROXY['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_HEALTHCARE_PROXY['ITEMID'] == 1703)]
# 	VAR_HEALTHCARE_PROXY = VAR_HEALTHCARE_PROXY[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_HEALTHCARE_PROXY.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_HEALTHCARE_PROXY

# def create_table_VAR_BED(mimic_db, POP):
# 	VAR_BED = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_BED = VAR_BED[(VAR_BED['CHARTTIME'] - VAR_BED['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_BED['ITEMID'] == 680)]
# 	VAR_BED = VAR_BED[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_BED.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_BED

# def create_table_VAR_SKIN_INTEGRITY(mimic_db, POP):
# 	VAR_SKIN_INTEGRITY = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_SKIN_INTEGRITY = VAR_SKIN_INTEGRITY[(VAR_SKIN_INTEGRITY['CHARTTIME'] - VAR_SKIN_INTEGRITY['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_SKIN_INTEGRITY['ITEMID'] == 644)]
# 	VAR_SKIN_INTEGRITY = VAR_SKIN_INTEGRITY[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_SKIN_INTEGRITY.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_SKIN_INTEGRITY

# def create_table_VAR_GIPROPHYLAXIS(mimic_db, POP):
# 	VAR_GIPROPHYLAXIS = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_GIPROPHYLAXIS = VAR_GIPROPHYLAXIS[(VAR_GIPROPHYLAXIS['CHARTTIME'] - VAR_GIPROPHYLAXIS['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_GIPROPHYLAXIS['ITEMID'] == 1427)]
# 	VAR_GIPROPHYLAXIS = VAR_GIPROPHYLAXIS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_GIPROPHYLAXIS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_GIPROPHYLAXIS

# def create_table_VAR_HEMOGLOBIN(mimic_db, POP):
# 	VAR_HEMOGLOBIN = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_HEMOGLOBIN = VAR_HEMOGLOBIN[(VAR_HEMOGLOBIN['CHARTTIME'] - VAR_HEMOGLOBIN['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_HEMOGLOBIN['ITEMID'] == 814)]
# 	VAR_HEMOGLOBIN = VAR_HEMOGLOBIN[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_HEMOGLOBIN.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_HEMOGLOBIN

# def create_table_VAR_PLATELETS(mimic_db, POP):
# 	VAR_PLATELETS = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_PLATELETS = VAR_PLATELETS[(VAR_PLATELETS['CHARTTIME'] - VAR_PLATELETS['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_PLATELETS['ITEMID'] == 828)]
# 	VAR_PLATELETS = VAR_PLATELETS[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_PLATELETS.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_PLATELETS

# def create_table_VAR_CPK(mimic_db, POP):
# 	VAR_CPK = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_CPK = VAR_CPK[(VAR_CPK['CHARTTIME'] - VAR_CPK['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_CPK['ITEMID'] == 784)]
# 	VAR_CPK = VAR_CPK[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_CPK.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_CPK

# def create_table_VAR_GUAIC(mimic_db, POP):
# 	VAR_GUAIC = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_GUAIC = VAR_GUAIC[(VAR_GUAIC['CHARTTIME'] - VAR_GUAIC['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_GUAIC['ITEMID'] == 660)]
# 	VAR_GUAIC = VAR_GUAIC[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_GUAIC.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_GUAIC

# def create_table_VAR_AMYLASE(mimic_db, POP):
# 	VAR_AMYLASE = POP.merge(mimic_db['CHARTEVENTS'], how='left', on='SUBJECT_ID') 
# 	VAR_AMYLASE = VAR_AMYLASE[(VAR_AMYLASE['CHARTTIME'] - VAR_AMYLASE['ICUSTAY_INTIME'] > datetime.timedelta(days=0, hours=0)) & \
# 	(VAR_AMYLASE['ITEMID'] == 775)]
# 	VAR_AMYLASE = VAR_AMYLASE[['SUBJECT_ID', 'CHARTTIME', 'VALUE1NUM']]
# 	VAR_AMYLASE.rename(columns={'VALUE1NUM' : 'VALUE'}, inplace=True)
# 	return VAR_AMYLASE

# Hardcode pull out a value for HR2. 
def HR_convert_to_value(string):
	if type(string) != str: return string
	string = string.replace(';', ':')
	string = string.replace(',', '.')
	string = string.replace('..', ':')
	splits = string.split(':')
	try:
		if len(splits) == 2 and len(splits[1]) > 0:
			return float(splits[1])
		return float(splits[0])
	except:
		return np.nan

# Function to handle strings in database columns!
# This honors NaN's and does not convert them.
def discretize_categories(dfcolumn):
	# Replace strings by digits... unsure what they are.
	uniqval = pd.unique(dfcolumn.values.ravel())
	uniqval = np.array([i for i in uniqval if (type(i) == str)])
	tags = np.array(range(len(uniqval)))
	return dfcolumn.replace(uniqval, tags)

# ----------------------------------------------------------------------------
