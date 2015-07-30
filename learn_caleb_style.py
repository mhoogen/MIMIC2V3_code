from learning import learn
from util_ import util

in_dir = '../../data/caleb/'
now = util.get_current_datetime()
out_dir = '../../data/learning_results/caleb/' + now
learn.execute(in_dir, out_dir, 'id', 'label', 'day', 1, ['LR'], True, False, '')