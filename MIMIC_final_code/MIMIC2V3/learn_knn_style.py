from learning import learnknn
from util_ import util

in_dir = ''

now = util.get_current_datetime()

out_dir = '' + now + '/'
learnknn.execute_knn(in_dir, out_dir, 'id', 'label', 'day', 1,1)
