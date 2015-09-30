import pandas as pd, numpy as np
import pickle, os, sys, datetime
from scipy.stats import mode, kurtosis
import mimic2pandas_datefix as Datefix

def cut_to_hour(datetime):
	return datetime.replace(minute=0, second=0, microsecond=0)

def addtypetoDF(df, typename, colname='TYPE'):
	size = df.shape[0] + 1
	df[colname] =  pd.Series([typename for i in range(size)])

def preprocess_combine_pop_cov(df, typename, timeaxis='CHARTTIME'):
	# Perform preprocessing on the files such that we homogenize to hours, AND we add a type before concatting
	# Let's first reindex the file
	if df.shape[0] > 0: 
		df = df.set_index([np.arange(1, df.shape[0] + 1)])
		addtypetoDF(df, typename)
		df[timeaxis] = df[timeaxis].apply(cut_to_hour) # Get rid of minutes, seconds, etc.
	return df

# Now we can actually merge!
def combine_covariates(VAR_TABLE_ARRAY):
	total = pd.concat(list(VAR_TABLE_ARRAY))
	total = total.set_index([np.arange(1, total.shape[0] + 1)])
	return total

# My plan is that for each individual, they don't stayin the ICU beyond a year, and I don't care about person X's age compared
# to person Y. I only care about the relationships as he develops. SO, i will scale all the years to a common one (which is leap)
# and then I will keep relativity

# I pick 2000 to be the start date b/c it's nice and relatively in the middle, and a leap year
def replace_with_fake_year(string, first):
	if type(string) != str: return string
	splits = string.split()
	dash_splits = splits[0].split('-')
	year = dash_splits[0]
	value = str(int(year) - int(first) + 2000)
	splits = ['-'.join([value] + dash_splits[1:])] + splits[1:]
	return ' '.join(splits)

def get_first_year(string):
	splits = string.split()
	dash_splits = splits[0].split('-')
	return dash_splits[0]

def shift_dates_in_range(total, attr, groupby_attr):
	total[attr] = pd.to_datetime(total[attr]).astype('str')
	regroup = []
	for i in total.groupby(groupby_attr):
		tmp = i[1].sort(attr) # sort by the charttime
		first_year = get_first_year(tmp[attr].iloc[0])
		tmp[attr] = tmp[attr].apply(lambda x: replace_with_fake_year(x, first_year))
		regroup.append(tmp)
	tmp = pd.concat(regroup)
	total = tmp.sort_index()
	# Successfuly converted to datetime!
	total[attr] = pd.to_datetime(total[attr], coerce=True)
	return total

# Function to handle strings in database columns!
# This honors NaN's and does not convert them.
def discretize_categories(dfcolumn):
	# Replace strings by digits... unsure what they are.
	uniqval = pd.unique(dfcolumn.values.ravel())
	uniqval = np.array([i for i in uniqval if (type(i) == str)])
	tags = np.array(range(len(uniqval)))
	return dfcolumn.replace(uniqval, tags)

# A function to initialize an empty N hour concatenated SVM
# and fill it out with whatever available data is in patient_df
def create_concat_FV(patient_df, order, start_time, size):
	# Make sure order is an array
	order = np.array(order)
	patient_FV = [[np.nan for i in order] for i in range(size)]
	grouped = patient_df.groupby('CHARTTIME')
	for group in grouped:
		deltatime = group[0] - start_time
		# print deltatime
		# Pull out the hours
		df_idx = int(deltatime.total_seconds() / float(3600))
		df = group[1] # Track the dataframe attached to this patients
		# For each df, we group it by type !
		nestgrouped = df.groupby(['TYPE'])
		for group2 in nestgrouped:
			if str(group2[0]) in order: # This way we exclude VASO
				# Grab the index where the attr type is supposed to be in our FV
				nestidx = np.where(order==str(group2[0]))[0][0]
				# Grab the mean of the range (If there's more than one)
				patient_FV[df_idx][nestidx] = np.mean(group2[1]['VALUE'])
	return np.array(patient_FV).flatten()

# A function to initialize an summary of a N hour slab 
# The functions for each features are :
# mean, min, max, std, median, mode, IQR, kurtosis

# This function internally handles interpolation and such, 
# Therefore, when using this, do not use the other interpolation. 
# Although this will have no nan's, so even doing so is harmless.
def create_stats_FV(patient_df, order, start_time):
	patient_FV = []
	order = np.array(order)
	for feature in order:
		# create a vector of size 7 because we're workign with 6 hours
		feat = list(np.zeros(6)) # This creates a sparseness of 0's
		feattimes = patient_df[patient_df['TYPE'] == feature]['CHARTTIME'].values.ravel()
		featvals = patient_df[patient_df['TYPE'] == feature]['VALUE'].values.ravel()
		for t in range(len(feattimes)):
			deltatime = feattimes[t] - start_time
			idx = deltatime.hours
			feat[idx] = featvals[t] 

		# Now let's interpolate this 
		for i in range(len(feat)):
			# The first value => sample mean => 0
			if feat[i] == 0 and i > 0:
				feat[i] = feat[i-1]

		# Now perform stats on it
		patient_FV = patient_FV + stats_suite(feat)
	return patient_FV	

def stats_suite(array):
	return [np.mean(array), np.min(array), np.max(array), np.std(array), \
			np.median(array), mode(array), get_IQR(array), kurtosis(array)]

def get_IQR(array):
	iqr = np.subtract(*np.percentile(x, [75, 25]))
	return iqr

def flatten_FV(feature_vectors, maxNaNcount=36):
	flatten_vectors = []
	for vec in feature_vectors:
		for nestedvec in vec:
			if sum(np.isnan(nestedvec)) < maxNaNcount: 
				flatten_vectors.append(nestedvec)
	flatten_vectors = np.array(flatten_vectors)
	return flatten_vectors



