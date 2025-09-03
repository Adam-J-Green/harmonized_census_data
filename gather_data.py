import data_processing as dp
import os
from configparser import ConfigParser


def main():

    config = ConfigParser()
    config.read('config.cfg')

    # create class instance for gatehering data
    proc = dp.DataBuilder()
    
    # get 2021 data if it has not yet been retrieved
    if not os.path.exists(config['files']['OUTPUT_FILE_2021']):
        print("Gathering 2021 Data")
        proc.get_dataset_2021()

    # get 2016 data if it has not yet been retrieved
    if not os.path.exists(config['files']['OUTPUT_FILE_2016']):
        print("Gathering 2016 Data")
        proc.get_dataset_2016()
    
    # combine datasets
    print("Generating Unified Dataset")
    final_data_long, final_data_wide = proc.combine_data()

    # write final datasets to working directory
    final_data_long.to_csv(config['files']['FINAL_DATA_LONG'])
    final_data_wide.to_csv(config['files']['FINAL_DATA_AGG'])

    os.remove(config['files']['OUTPUT_FILE_2016'])
    os.remove(config['files']['OUTPUT_FILE_2021'])
    

if __name__ == "__main__":
    main()