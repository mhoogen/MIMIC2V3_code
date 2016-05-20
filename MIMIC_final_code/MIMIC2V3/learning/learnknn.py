from util_ import util
from util_ import in_out
import algorithms as ML
from sklearn.feature_selection import SelectKBest, f_classif, chi2, VarianceThreshold
import numpy as np
from scipy.stats import pearsonr
from sklearn.metrics import roc_curve, auc, confusion_matrix
from scipy import interp, spatial
from util_ import dtw
from util_ import fastdtw
from util_ import kmeans
from numpy.linalg import norm
from numpy import *
from util_ import clustering
import math
import time
from sklearn.preprocessing import normalize
from sklearn.cross_validation import StratifiedKFold


dtw_attr = ['hr', 'resp', 'nbp', 'sbp', 'dbp', 'so2']
window = 4

def execute_knn(in_dir, out_dir, record_id, target_id, day_id, day, k):
	'''executes the learning task on the data in in_dir with the algorithms in algorithms.
		The results are written to out_dir and sub_directories,
	    and the record_ and target_ids are used to differentiate attributes and non-attributes'''
	print '### executing learning algorithms on... ###'

	# get the files
	files = util.list_dir_csv(in_dir)

	# stop if no files found
	if not files:
		print 'No appropriate csv files found. Select an input directory with appropriate files'
		return

	# create directory
	util.make_dir(out_dir)

	# execute each algorithm

	# run algorithm alg for each file f
	for f in files:
		results_list = []
		fname = in_out.get_file_name(f, extension=False)
		print ' ...{}'.format(fname)
		
		# get data, split in features/target. If invalid stuff happened --> exit
		X, y, headers = in_out.import_data(f, record_id, target_id, True) # assumption: first column is patientnumber
		if type(X) == bool: return

		day_index = headers.index(day_id)
		new_X = np.zeros((0, len(headers)))
		new_y = []

		IDs = []
		IDrows = {}

		# ordering of time points and complete data (filled with nan's if not available) assumed!
		
		
#  		features_to_be_removed   =    [ "pvc_bin","pnc_bin","pac_bin","ect_freq_bin","full_code_bin","comfort_meas_bin","other_code_bin","no_cpr_bin",
# 										"dnr_bin","dni_bin","fall_risk_bin","orientation_ord","orient_unable_ass_bin","riker_sas_ord","vent_bin",
# 										"vent_mode_ord","pacemaker_bin","trach_bin","flush_skin_bin","jaundice_skin_bin","pale_skin_bin","impaired_skin_bin",
# 										"iabp_ord","iabp_bin","svnsicu_bin","svcsicu_bin","svcsru_bin","svmicu_bin","svmsicu_bin","svother_bin","svccu_bin",
# 										"gender"] 
	
		exclude = [146,140,95,123,88,133,22,65,49,114,178,55,133,138,34,186,20,73]
		new_index = 0
		for i in range(0, X.shape[0]):
			if X[i,headers.index(day_id)] == day or day == -1:
				row = np.array(X[i,:]).reshape(-1)

				if not row[0] in IDs and not row[0] in exclude:
					IDs.append(row[0])
					new_y.append(int(y[i]))
					IDrows[row[0]] = [new_index]
				elif not row[0] in exclude:
					IDrows[row[0]].append(new_index)
				new_X = np.append(new_X, np.column_stack(row), axis=0)
				new_index += 1
		
		ID_column = new_X[:,0]
		
		# Remove the id, the day, and the time stamp from the data and headers.
		new_X = np.delete(new_X, 2, 1)
		new_X = np.delete(new_X, 1, 1)
		new_X = np.delete(new_X, 0, 1)
		new_headers = headers[3:len(headers)]
		
		dtw_attr = ['hr', 'resp', 'nbp', 'sbp', 'dbp', 'so2']
            
		X = new_X
		print len(X)
				
		non_singular_rows = [i for i in range(0, X.shape[1]) if len(set(util.get_non_nans(X[:,i].tolist()))) > 1 ]
		#print str(len(non_singular_rows)) + ' ' + str(X.shape[1])
		#print non_singular_rows

		X = X[:,non_singular_rows]
		new_headers = np.array(new_headers)[non_singular_rows].tolist()
		print str(len(new_headers)) + "length new headers after non singular rows"
		print new_headers
		print "Removed columns with only nan of 1 value"
		max_values = np.nanmax(X, axis=0)
		min_values = np.nanmin(X, axis=0)

		ranges = []
		for i in range(0, len(min_values)):
			diff = max_values[i] - min_values[i]
			if diff == 0:
				print 'difference of zero encountered in ' + str(i)
				print 'Max values: ' + str(max_values[i])
				print 'Min values: ' + str(min_values[i])
				ranges.append(1)
			else:
				ranges.append(diff)

		# Now do some scaling to get the values to the same order or magnitude
		scaled_X = (X - min_values)/(max_values - min_values)
		X = scaled_X
		y = np.squeeze(np.asarray(new_y))
		
		print "Scaling done!"
		
		new_IDrows = {}
		for ID in IDs:
			IDrows[ID] = {'first_row':min(IDrows[ID]), 'last_row':max(IDrows[ID])}

		print '  ...instances: {}, attributes: {}'.format(X.shape[0], X.shape[1])

		# Now we are going to build the similarity matrix. We are also going to store how many attributes
		# we actually able to make a comparison for.

		similarity_matrix = np.ones(( len(IDs)  ,  len(IDs) ))
		matching_number_matrix = np.ones((len(IDs),len(IDs)))
			
		for attr in range(0, len(new_headers)):
			print str(attr) + "attribute in KNN loop" 
			print str(attr) + "/" + str(len(new_headers))
				
			temp = np.ones((len(IDs), len(IDs)))
			temp[:] = 2
			for i in range(0, len(IDs)):
				for j in range(i+1, len(IDs) ):
					
					i_data = X[IDrows[IDs[i]]['first_row']:IDrows[IDs[i]]['last_row']+1,attr].tolist()
					j_data = X[IDrows[IDs[j]]['first_row']:IDrows[IDs[j]]['last_row']+1,attr].tolist()
			
					if new_headers[attr] in dtw_attr:
						dtw_distance = dtw.lb_keogh(i_data, j_data, window)
							
						if not dtw_distance == -1:
							temp[i,j] += dtw_distance
							matching_number_matrix[i,j] += 1
							matching_number_matrix[j,i] = matching_number_matrix[i,j]
							temp[j,i] = temp[i,j]
					else:
						i_data = util.get_non_nans(i_data)
						j_data = util.get_non_nans(j_data)
						if len(i_data) > 0 and len(j_data) > 0:
							simple_distance = math.pow(np.mean(i_data)- np.mean(j_data), 2)
							temp[i,j] += simple_distance 
							matching_number_matrix[i,j] += 1
							matching_number_matrix[j,i] = matching_number_matrix[i,j]
							temp[j,i] = temp[i,j]			
			
	 		if np.max(temp) != 0:
	 			temp = temp / np.max(temp)			
			similarity_matrix += temp
		
		# We calculate the average score per item matched
		# Best might be to apply a weighting scheme now.
		similarity_matrix = (similarity_matrix / matching_number_matrix) + (1 / matching_number_matrix)
			
		print len(IDs)
		results = perform_classification(similarity_matrix, y, out_dir, k)
		results_list.append(results)
		print results
		in_out.save_results(out_dir+str(k)+'.csv', ["fpr", "tpr", "auc", "cm"], results[1:len(results)], [sum(y),len(y)])
		in_out.save_ROC(out_dir + '/roc.png', results_list, title='ROC curve')
		
		# notify user
	print '## Learning Finished ##'
	print similarity_matrix

def perform_classification(similarity_matrix, y, out_dir, k):
    cv = StratifiedKFold(y, n_folds=5) # x-validation

    mean_tpr = 0.0
    mean_fpr = np.linspace(0, 1, 100)
    all_tpr = []
    cm=np.zeros((2,2))

    # cross fold validation
    print '  ...performing x-validation'
    # part to store eventual class of the patients can easily be removed...

    for i, (train, test) in enumerate(cv):
        print '   ...',i+1
        if sum(y[train]) < 1:
            print '...cannot train; too few positive examples'
            return False, False

        training_sim_matrix = similarity_matrix[:,train]
        training_y = y[train]
        y_pred = np.ndarray((0,1))

        for p in test:
        	# Get the patients in the training set with the highest scores
        	scores = training_sim_matrix[p,:]
        	index_lowest_scores = np.argsort(training_sim_matrix[p,:])[:k]
        	y_pred = np.append(y_pred, float(sum(training_y[index_lowest_scores]))/k)

        # make cutoff for confusion matrix
        y_pred_binary = [1 if y_pred[i] >= 0.7 else 0 for i in range(0, len(y_pred))]

        # derive ROC/AUC/confusion matrix
        fpr, tpr, thresholds = roc_curve(y[test], y_pred)
        mean_tpr += interp(mean_fpr, fpr, tpr)
        mean_tpr[0] = 0.0
        cm = cm + confusion_matrix(y[test], y_pred_binary)

    mean_tpr /= len(cv)
    mean_tpr[-1] = 1.0
    mean_auc = auc(mean_fpr, mean_tpr)
    mean_cm = cm/len(cv)



    return ['k='+str(k), mean_fpr, mean_tpr, mean_auc, mean_cm]


