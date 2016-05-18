from learning import learn
from util_ import util

in_dir = 'C:/Users/ali_e_000/Desktop/Research Paper/learn/test-lr'
now = util.get_current_datetime()
out_dir = 'C:/Users/ali_e_000/Desktop/Research Paper/' + now
learn.execute(in_dir, out_dir, 'id', 'label', 'day', 1, ['LR'], True, False, '')