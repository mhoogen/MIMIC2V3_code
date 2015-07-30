from data_processing.process_data import createProcessedFiles
from util_ import in_out as io

c = createProcessedFiles()
[attr, values] = c.read_files_and_calculate_attributes('/scratch/mimic_dump/mimic2v26_rohit_no_hold.csv', '../data/export_file_caleb.csv', 0)
# [attr, values] = c.read_files_and_calculate_attributes('/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/temp.csv', '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/processed/caleb/export_file_caleb.csv', 0)
