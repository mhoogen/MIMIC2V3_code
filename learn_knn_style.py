from learning import learnknn
from util_ import util

in_dir = '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/processed/knn'
now = util.get_current_datetime()
out_dir = '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/learning_results/knn/' + now + '/'
learnknn.execute_knn(in_dir, out_dir, 'id', 'label', 'day', 1, 5)