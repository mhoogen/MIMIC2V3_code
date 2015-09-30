# After splitting all the partitions, they might be small enough to join them together

import pandas as pd, numpy as np
import pickle, os, sys

mimic_table_names = ['A_CHARTDURATIONS', 'ADDITIVES', 'ADMISSIONS', 'A_IODURATIONS', 'A_MEDDURATIONS', 'CENSUSEVENTS', 'CHARTEVENTS', 'COMORBIDITY_SCORES', 'DELIVERIES', 'DEMOGRAPHIC_DETAIL', 'DEMOGRAPHICEVENTS', 'D_PATIENTS', 'DRGEVENTS', 'ICD9', 'ICUSTAY_DAYS', 'ICUSTAY_DETAIL', 'ICUSTAYEVENTS', 'IOEVENTS', 'LABEVENTS', 'MEDEVENTS', 'MICROBIOLOGYEVENTS', 'NOTEEVENTS', 'POE_MED', 'POE_ORDER', 'PROCEDUREEVENTS', 'TOTALBALEVENTS']

result = {}
for table in mimic_table_names:
	result[table] = []
result = pd.Series(data=result)

path = '/scratch/mimic-ii/extracted-data'
to_combine = os.listdir(path)
to_combine = [os.path.join(path, i) for i in to_combine]

array_of_df = []
for partition in to_combine:
	print "Reading in partition %s..." % partition
	array_of_df.append(pd.read_pickle(partition))

# Array_of_df actually is an array of Series objects...
# In order to concat this, we have to concat each one
for table in mimic_table_names:
	print "Postprocessing for table index %s..." % table
	result[table] = pd.concat([i[table] for i in array_of_df])
	result[table] = result[table].set_index([np.arange(1, result[table].shape[0] + 1)])

print "Saving to pickle..."
result.to_pickle(os.path.join(path, 'full_mimic_pandas_db.pkl'))
