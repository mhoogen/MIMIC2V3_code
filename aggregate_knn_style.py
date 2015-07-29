from data_processing.process_data import createProcessedFiles
from util_ import in_out as io

c = createProcessedFiles()
[attr, values] = c.read_files_and_calculate_attributes('/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/small_export.csv', 2)
print values.shape
writer = io.write_csv('/Users/markhoogendoorn/Documents/Mijn Research Documenten/MIMIC-II/V3.0_export/processed/knn/export.csv')
writer.writerow(attr)
for row in values:
    writer.writerow(row)