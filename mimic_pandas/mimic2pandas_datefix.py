# After creating these pandas structures. We have to retroactively 
# fix up the the dates from strings to np.datetime64
# This assumes you have some pandas pickle file with the structure in it,
# And it will return a pickle file with an edited structure. 

import pandas as pd, numpy as np # Will be using numpy arrays
import sys, os, pickle, datetime

# ------------------------------------------------------------------------

# One option (instead of removing years), is to use a 
# data structure that can handle huge years ==> datetime object
# This makes it a little easier since it provides some constancy!


def convert_datetime_df(DF, ColumnName):
    # For this I think I only need long ones
    DF[ColumnName] = DF[ColumnName].apply(to_datetime_format)
    return DF

def convert_trunc_datetime_df(DF, ColumnName):
    # For this I think I only need long ones
    DF[ColumnName] = DF[ColumnName].apply(to_trunc_datetime_format)
    return DF

def to_datetime_format(i):
    if type(i) == datetime.datetime: return i
    if type(i) != str and np.isnan(i): return i
    timearray = i.split()
    datetimeInputs = timearray[0].split('-') + timearray[1].split(':')
    datetimeInputs = [int(i) for i in datetimeInputs]
    year = datetimeInputs[0]
    month = datetimeInputs[1]
    day = datetimeInputs[2]
    hour = datetimeInputs[3]
    minute = datetimeInputs[4]
    second = datetimeInputs[5]
    return datetime.datetime(year, month, day, hour, minute, second)

def to_trunc_datetime_format(i):
    if type(i) == datetime.datetime: return i
    if type(i) != str and np.isnan(i): return pd.to_datetime(i)
    datetimeInputs = i.split('-')
    datetimeInputs = [int(i) for i in datetimeInputs]
    year = datetimeInputs[0]
    month = datetimeInputs[1]
    day = datetimeInputs[2]
    hour = 0
    minute = 0
    second = 0
    return datetime.datetime(year, month, day, hour, minute, second)
# -----------------------------------------------------
# Post-processing fix : AKA Only fix the date columns
# that I am using, therefore we don't want to deal with
# problems that I don't have to...

# Example of usage : 
# -- This line remembers the starting points for all the ICUSTAY_ID in demographic
# patients_time_diff = remember_first_date(DEMOGRAPHIC)
# -- These align all the dates such that we only care about preserving relative distance.
# -- We don't care about difference between patients (since we are always comparing within patients)
# DEMOGRAPHIC = post_process_date_align(DEMOGRAPHIC, patients_time_diff, "ICUSTAY_INTIME", "SUBJECT_ID")
# DEMOGRAPHIC = post_process_date_align(DEMOGRAPHIC, patients_time_diff, "ICUSTAY_OUTTIME", "SUBJECT_ID")
# VASO = post_process_date_align(VASO, patients_time_diff, "CHARTTIME", "SUBJECT_ID")

def remember_first_date(DEMOGRAPHIC):
    # time_min = np.datetime64('1700-01-01T00:00:00Z') # Standard for what I'm setting <-- near the minimum
    time_min = datetime.datetime(1700, 01, 01, 00, 00)
    # Before any editing, build the system to remember ICUSTAY dates
    patients = np.unique(DEMOGRAPHIC["SUBJECT_ID"].values.ravel())
    patients_time_diff = {}
    for p in patients:
        p_init_time = DEMOGRAPHIC[DEMOGRAPHIC["SUBJECT_ID"] == p]["ICUSTAY_INTIME"].min()
        p_diff_time =  p_init_time - time_min
        patients_time_diff[p] = p_diff_time
    
    return patients_time_diff

def force_subtract_date(i, j):
    try:
        return i - j
    except:
        return pd.to_datetime(np.nan)

def post_process_date_align(DF, patients_time_diff, timeColumn, groupbyColumn='SUBJECT_ID'):
    grouped = DF.groupby(groupbyColumn, as_index=False)
    changed_group = []
    for group in grouped:
        patientID = group[0]
        patientDF = group[1]
        # changed_group.append(pd.to_datetime(patientDF[timeColumn].apply(lambda x: x - patients_time_diff[patientID]), coerce=True))
        changed_group.append(pd.to_datetime(patientDF[timeColumn].apply(lambda x: force_subtract_date(x, patients_time_diff[patientID]))))
    
    # Make sure it is a series so we can do easy replacement.
    changed_df = pd.Series(pd.concat(changed_group))
    DF[timeColumn] = pd.Series(changed_df)
    return DF


