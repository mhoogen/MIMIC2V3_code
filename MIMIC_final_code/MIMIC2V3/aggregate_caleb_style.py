from data_processing.process_data import createProcessedFiles
from util_ import in_out as io

c = createProcessedFiles()

[attr, values] = c.read_files_and_calculate_attributes('C:/Users/ali_e_000/Desktop/data_mimic_mark/outcome_result.csv', 'C:/Users/ali_e_000/Desktop/Research Paper/knn/500000_caleb_new.csv', 0)
# [attr, values] = c.read_files_and_calculate_attributes('/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/temp.csv', '/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/processed/caleb/export_file_caleb.csv', 0)

