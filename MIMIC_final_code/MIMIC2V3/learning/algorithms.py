# os-related imports
import sys
import csv
from datetime import datetime

# numpy
import numpy as np

# algorithms
from sklearn import ensemble, svm, tree, linear_model

# statistics, metrics, x-fold val, plots
from sklearn.metrics import roc_curve, auc, confusion_matrix
from sklearn.cross_validation import StratifiedKFold
from sklearn.pipeline import Pipeline

from scipy import interp

def SVM(X, y, best_features):
    clf = svm.SVC(verbose=True, shrinking=False, probability=True, cache_size=1500, class_weight='auto')
    e_clf = ensemble.BaggingClassifier(clf, n_estimators=1, max_samples = 0.2, n_jobs=-1, verbose=True)
    results, model = execute(X, y, best_features, lambda: e_clf)
    return results, model

def CART(X, y, best_features, out_file, field_names):
    results, model = execute(X, y, best_features, lambda: tree.DecisionTreeClassifier(max_depth=5, min_samples_leaf=50))
    if model:
        tree.export_graphviz(model, out_file=out_file, feature_names=field_names)
    return results, model

def RF(X, y, best_features, n_estimators):
    results, model = execute(X, y, best_features, lambda: ensemble.RandomForestClassifier(n_estimators=n_estimators,max_depth=5, min_samples_leaf=50, n_jobs=-1))
    if model:
        features = model.feature_importances_
    else:
        features = False
    return results, features, model

def LR(X, y, best_features):
    results, model = execute(X, y, best_features, lambda: linear_model.LogisticRegression(C=150, dual = False ,penalty='l2', tol=1e-6,))  #C = 1000, dual=False,penalty='l2')
    if model:
        features = model.coef_
    else:
        features = False
    return results, features, model

def execute(X, y, best_features, classifier):
    cv = StratifiedKFold(y, n_folds=5) # x-validation
    classifier = classifier()

    # print np.var(X[:,0]),np.var(X[:,1]),np.var(X[:,2]),np.var(X[:,3]),np.var(X[:,4]),np.var(X[:,5]),

    clf = Pipeline([('classifier',classifier)])

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

        # train
        if type(clf.named_steps['classifier']) == tree.DecisionTreeClassifier:
            num_pos = sum(y[train]) # number of positive cases
            w0 = num_pos / len(y[train]) # inversely proportional with number of positive cases
            w1 = 1 - w0 # complement of w0
            sample_weight = np.array([w0 if el==0 else w1 for el in y[train]])
            trained_classifier = clf.named_steps['classifier'].fit(X[train], y[train], sample_weight=sample_weight)
            #trained_classifier = clf.fit(X[train], y[train], sample_weight=sample_weight)
        else:
            trained_classifier = clf.fit(X[train], y[train])

        # test
        # if best_features == 'all': # if we did not do feature selection
        y_pred = trained_classifier.predict_proba(X[test])
        # else: # if we performed feature selection, we have a list of indices indicating the best features
            # y_pred = trained_classifier.predict_proba(X[test][:,best_features])

        # make cutoff for confusion matrix
        y_pred_binary = (y_pred[:,1] > 0.01).astype(int)


        # derive ROC/AUC/confusion matrix
        fpr, tpr, thresholds = roc_curve(y[test], y_pred[:, 1])

        mean_tpr += interp(mean_fpr, fpr, tpr)
        mean_tpr[0] = 0.0
        cm = cm + confusion_matrix(y[test], y_pred_binary)

    mean_tpr /= len(cv)
    mean_tpr[-1] = 1.0
    mean_auc = auc(mean_fpr, mean_tpr)
    mean_cm = cm/len(cv)


    # redo with all data to return the features of the final model
    print '  ...fitting model (full data sweep)'.format(X.shape)
    complete_classifier = clf.fit(X,y)

    # Optional part for analysis of the patients.
    y_pred = complete_classifier.predict_proba(X)
    fpr, tpr, thresholds = roc_curve(y, y_pred[:,1])

    return [mean_fpr, mean_tpr, mean_auc, mean_cm], complete_classifier.named_steps['classifier']
