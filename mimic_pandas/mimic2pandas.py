# A script to convert the mimic csv downloads to a Pandas DataFrame
# Written by Mike Wu 

import pandas as pd, numpy as np # Will be using numpy arrays
import sys, os

# Find which mimic table index the file belongs to
def which_mimic_table(test_file, table_names):
    for table in table_names:
        if table in test_file:
            return table

def convert2pandas():
    # For each subject, there are 32 data tables provided
    mimic_table_names = ['A_CHARTDURATIONS', 'ADDITIVES', 'ADMISSIONS', 'A_IODURATIONS', 'A_MEDDURATIONS', 'CENSUSEVENTS', 'CHARTEVENTS', 'COMORBIDITY_SCORES', 'DELIVERIES', 'DEMOGRAPHIC_DETAIL', 'DEMOGRAPHICEVENTS', 'D_PATIENTS', 'DRGEVENTS', 'ICD9', 'ICUSTAY_DAYS', 'ICUSTAY_DETAIL', 'ICUSTAYEVENTS', 'IOEVENTS', 'LABEVENTS',
        'MEDEVENTS', 'MICROBIOLOGYEVENTS', 'NOTEEVENTS', 'POE_MED', 'POE_ORDER', 'PROCEDUREEVENTS', 'TOTALBALEVENTS']
    # Initialize a wrapper pd for each future pd name
    d = {}
    for table_name in mimic_table_names:
        d[table_name] = [] # Initialize the series wrapper to empty
    mimic_db = pd.Series(data=d) 

    mimic_environ = '/scratch/mimic-ii/rawfiles/' 
    mimic_file_ignore = ['.DS_Store'] # Ignore DS STORES
    # Hardcode the partition nums to be '00' --> '32'
    mimic_partition_num = np.array([str(i) for i in range(0, 33)])
    mimic_partition_num[0:10] = ['0' + i for i in mimic_partition_num[0:10]]
    # Pick where to save pickle file
    mimic_savetofolder = '/scratch/mimic-ii/picklefiles/'
    mimic_savetofile = 'mimic_pandas_db.pkl'

    first_partition = True # boolean to see if first partition
    for mimic_partition in mimic_partition_num:
        print 'Begin Processing for Partition %s...' % mimic_partition
        print '====================================\n'
        mimic_partition_contents = [folder for folder in os.listdir(os.path.join(mimic_environ,mimic_partition)) \
                                    if ((folder not in mimic_file_ignore) and ('._' not in folder))]
        mimic_partition_contents = np.array(mimic_partition_contents)
        mimic_partition_contents = np.sort(mimic_partition_contents)

        # tmporary storage because editing mimic_db directly doesn't fly well
        tmp = {}
        for table_name in mimic_table_names:
            tmp[table_name] = [] # Initialize the series wrapper to none 
        
        partition_content_counter = 0 
        for curr_partition_content in mimic_partition_contents:
            partition_content_counter += 1
            print 'Processing partition %s... (%d / %d)' % (curr_partition_content, partition_content_counter, len(mimic_partition_contents))
            # Grab all the name of the files in that content
            mimic_content_files = [files for files in os.listdir(os.path.join(mimic_environ, mimic_partition, curr_partition_content)) \
                                   if ((files not in mimic_file_ignore) and ('._' not in files))]
            # For each file in each folder, create a pd out of it!
            for mimic_file in mimic_content_files:
                mimic_file_path = os.path.join(mimic_environ, mimic_partition, curr_partition_content, mimic_file)
                if os.path.getsize(mimic_file_path) > 0: # Make sure the file is not empty
                    # Add it to the mimic DB
                    mimic_db_index = which_mimic_table(mimic_file, mimic_table_names) 
                    tmp[mimic_db_index].append(pd.read_csv(mimic_file_path))
                    
        if first_partition: 
            for mimic_db_index in mimic_table_names:
                mimic_db[mimic_db_index] = pd.concat(tmp[mimic_db_index])
            first_partition = False
        else: # if not the first partition, then we have to be smarter 
            for mimic_db_index in mimic_table_names:
                copy_mimic = mimic_db[mimic_db_index] # Grab the current state 
                tmp[mimic_db_index] = [copy_mimic] + tmp[mimic_db_index]
                mimic_db[mimic_db_index] = pd.concat(tmp[mimic_db_index])
    
    print 'Postprocessing to prepare Pandas Dataframe...'
    # Fix the indicies for all DB
    for mimic_db_index in mimic_table_names:
        mimic_db[mimic_db_index] = mimic_db[mimic_db_index].set_index([np.arange(1, mimic_db[mimic_db_index].shape[0] + 1)])
    
    # Great, now everything is indexible and we have replicated a database.
    # save this wrapper object as a pickle for later access. 
    print 'Saving to specified pickle file...'
    mimic_db.to_pickle(open(os.path.join(mimic_savetofolder, mimic_savetofile), 'wb'))
    print '---------------------------------------------'
    print 'Successfully converted MIMIC to Pandas files.'
    print '---------------------------------------------\n'

    return mimic_db

