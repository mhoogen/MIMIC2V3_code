# This function is a parallel function with partition_looper.py. Because 
# partition_looper.py creates so many files of each type for each partition,
# we need to somehow JOIN the CSV files manually! (which is actually easy)
# Note that it is easier to join files manually, then with pickle objects
# or something 

import numpy as np

# Let's define some settings
tmp = np.arange(0, 33)
tmp = [str(i) for i in tmp]
tmp[0:10] = ['0' + i for i  in tmp[0:10]]
file_syntax = 'partition-'
file_ending = '.csv'
path_syntax = '/n/dtak/mimic-ii/test-data/'
save_path_syntax = '/n/dtak/mimic-ii/query-data/'

# These are the names we need to join
# nameList = ['POP', 'VASO', 'SEDATIVE', 'MEAN_BP', 'TEMP', 'HR', 'SPO2', 'FIO2', \
# 		'SPONTANEOUS_RR', 'URINE', 'BUN', 'CREATININE', 'GLUCOSE', \
# 		'BICARBONATE', 'HCT', 'LACTATE', 'MAGNESIUM', 'PLATELETS', \
# 		'POTASSIUM', 'SODIUM', 'WBC']
nameList = ['HOSPITAL_EXPIRE_FLG']
# For each type of variables
for name in nameList:
	print "Joining files for attributes: %s." % name
	# Generate the list of full paths... 
	fullpaths = [path_syntax + file_syntax + i + '-' + name + file_ending for i in tmp]
	# We want to make a new file (cf = combinedfile)
	fout = open(save_path_syntax + name + file_ending, 'a')
	# Do the first file separately. 
	# For each line in the first file, write it to the new file
	for line in open(fullpaths[0]):
		fout.write(line)
	# Now do the same for all future files
	for path in fullpaths[1:]:
		f = open(path)
		# for these files, read the first line and drop it (these are the tags)
		f.next() # Skip header
		for line in f:
			fout.write(line)
		f.close()
	fout.close()
	# Now we should have a file called ATTRIBUTE.csv (that contains it all!)


