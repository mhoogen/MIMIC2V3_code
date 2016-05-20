from data_processing.process_data import createProcessedFiles
from util_ import in_out as io

c = createProcessedFiles()

[attr, values] = c.read_files_and_calculate_attributes('/outcome_result.csv', 'data.csv', 2)
