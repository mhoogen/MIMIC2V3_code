from learning import learnknn
from util_ import util

#in_dir = '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/processed/knn'
in_dir = 'C:/Users/ali_e_000/Desktop/Research Paper/learn/test-knn'

now = util.get_current_datetime()
#out_dir = '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/learning_results/knn/' + now + '/'
out_dir = 'C:/Users/ali_e_000/Desktop/Research Paper/' + now + '/'
learnknn.execute_knn(in_dir, out_dir, 'id', 'label', 'day', 1,1)
