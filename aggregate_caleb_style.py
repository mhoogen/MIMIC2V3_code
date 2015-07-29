from data_processing.process_data import createProcessedFiles
from util_ import in_out as io

c = createProcessedFiles()
[attr, values] = c.read_files_and_calculate_attributes('/scratch/mimic_dump/mimic2v26_rohit_no_hold.csv', 0)
print values.shape
writer = io.write_csv('../data/export_file_caleb.csv')
writer.writerow(attr)
for row in values:
    writer.writerow(row)
