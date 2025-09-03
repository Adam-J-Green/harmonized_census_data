import data_processing as dp
import os
from configparser import ConfigParser


def main():
    config = ConfigParser()
    config.read('config.cfg')
    proc = dp.DataBuilder()
    
    if not os.path.exists(config['files']['OUTPUT_FILE_2021']):
        proc.get_dataset_2021()

    if not os.path.exists(config['files']['OUTPUT_FILE_2016']):
        proc.get_dataset_2016()
    
    final_data_long, final_data_wide = proc.combine_data()

    final_data_long.to_csv(config['files']['FINAL_DATA_LONG'])
    final_data_wide.to_csv(config['files']['FINAL_DATA_AGG'])
    

if __name__ == "__main__":
    main()