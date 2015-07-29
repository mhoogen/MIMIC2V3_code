from learning import learn
from util_ import util

in_dir = '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/processed/caleb/'
now = util.get_current_datetime()
out_dir = '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/learning_results/caleb/' + now
learn.execute(in_dir, out_dir, 'id', 'label', 'day', 1, ['LR'], True, False, '')