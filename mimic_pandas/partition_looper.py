# This file is meant for me to use as a wrapper function such that for every thing I want to run as a function, I can just pass it to this and it will run successfully for each partition. 
# -----------------------------------------------------------------------------
# The point is that for each partition I'm going to save the result as a CSV. Separately I can join all the CSV's, and then hopefully load all of those together into memory!

import pickle
import pandas as pd, numpy as np
import sys
sys.path.append('../')
import mimic_sql_queries as psql

# Because of resource constraints, the database that 
# we are querying from is split into several Pandas structures. Therefore,
# although the queries are written as if they are querying a single DB,
# we must define a wrapper function to query and combine from all partitions.

# query fxn - is any function found below
# query args - any addition arguments needed for the query 
#			   does NOT contain the mimic_db itself. 
# mimic_pkls - an array of pickle files
# def loop_partitions(mimic_pkls, queryfxn, *queryargs):
# 	# loop through the pickle files, and then operate on them
# 	fileNumber = 0
# 	for pklfile in mimic_pkls:
# 		print "Loading partition file number %d." % fileNumber
# 		partition_db = pickle.load(open(pklfile, 'rb'))
# 		# Run the queryfxn on the partition database
# 		result = queryfxn(partition_db, *queryargs)
# 		# result should be a pandas db
# 		if type(result) == pd.core.frame.DataFrame:
# 			# save it to a csv file	
# 			print "Saving result to file %d." % fileNumber
# 			result.to_csv('result' + str(fileNumber) + '.csv')
# 			fileNumber += 1 # Increment to next file. 

# But instead I wanna do very specific things for looping
# through partitions -- 

# This will grab us 1) POP, all the vital signs, and all the lab data, and outcomes
print 'here. Beginning function'
# Generate the pickle paths
tmp = np.arange(0, 33)
tmp = [str(i) for i in tmp]
tmp[0:10] = ['0' + i for i  in tmp[0:10]]
file_syntax = 'mimic_pandas_db-'
file_ending = '.pkl'
path_syntax = '/scratch/mimic-ii/extracted-data/'
fullpaths = [path_syntax + file_syntax + i + file_ending for i in tmp]

# Start the loop
count = 0
for pklpath in fullpaths:
	print 'Reading in partition name: %s' % pklpath
	partition_db = pd.read_pickle(pklpath)
	print 'Finished reading in partition. Making queries.'
	# Do the POP query
	POP = psql.create_table_POP(partition_db)
	print 'Finished POP query.'
	# Get the outcome data
	OUTCOMES = psql.create_outcomes_tables(partition_db, POP)
	print 'Finished VASO and SEDATIVE queries.'
	
	# Get the Vital Signs Table
	# VAR_MEAN_BP, VAR_TEMP, VAR_HR, VAR_SPO2, VAR_FIO2, \
	# VAR_SPONTANEOUS_RR, VAR_URINE = psql.create_vitals_tables(partition_db, POP)
	# print 'Finished Vital Signs queries.'
	# Get the Lab results data
	# VAR_BUN, VAR_CREATININE, VAR_GLUCOSE, VAR_BICARBONATE, VAR_HCT, \
	# VAR_LACTATE, VAR_MAGNESIUM, VAR_PLATELETS, VAR_POTASSIUM, VAR_SODIUM, \
	# VAR_WBC = psql.create_lab_results_tables(partition_db, POP)
	# print 'Finished Lab Results queries.'

	# nameList = ['POP', 'VASO', 'SEDATIVE', 'MEAN_BP', 'TEMP', 'HR', 'SPO2', 'FIO2', \
	#		'SPONTANEOUS_RR', 'URINE', 'BUN', 'CREATININE', 'GLUCOSE', \
	#		'BICARBONATE', 'HCT', 'LACTATE', 'MAGNESIUM', 'PLATELETS', \
	#		'POTASSIUM', 'SODIUM', 'WBC']
	
	# Save everything to stuff. 
	#varlist = [POP, VASO, SEDATIVE, VAR_MEAN_BP, VAR_TEMP, VAR_HR, VAR_SPO2, VAR_FIO2, \
	#			VAR_SPONTANEOUS_RR, VAR_URINE, VAR_BUN, VAR_CREATININE, VAR_GLUCOSE, \
	#			VAR_BICARBONATE, VAR_HCT, VAR_LACTATE, VAR_MAGNESIUM, VAR_PLATELETS, \
	#			VAR_POTASSIUM, VAR_SODIUM, VAR_WBC]


	# July 30th Edit:
	nameList = ['MORTALITY']
	varlist = [HOSPITAL_EXPIRE_FLG]

	for i in range(len(varlist)):
		print 'Saving %s to csv.' % nameList[i]
		varlist[i].to_csv('/scratch/mimic-ii/test-data/partition-' + tmp[count] + '-' + nameList[i] + '.csv')

	count += 1
