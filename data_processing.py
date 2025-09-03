import pandas as pd
import requests
import json
import os
from zipfile import ZipFile
from configparser import ConfigParser
import ast
from typing import Tuple

class DataBuilder():
    def __init__(self):
        self.config = ConfigParser()
        

    def get_dataset_2021(self):

        # retrieve file locations and data query string from config file
        self.config.read('config.cfg')
        query_string = self.config['querys']['QUERY_URL_2021']
        zip_location = self.config['files']['ZIP_LOCATION_2021']
        output_filename = self.config['files']['OUTPUT_FILE_2021']

        try:
            # API request to download zip file containing census data
            print('downloading 2021 census data')
            response = requests.get(query_string, stream=True)
            response.raise_for_status() 

            # write returned data to zip location of interest
            with open(zip_location, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"File '{zip_location}' downloaded successfully.")

        except requests.exceptions.RequestException as e:
            print(f"Error during download: {e}")

        # Extract main datafile from zip folder to working directory
        with ZipFile(zip_location, 'r') as zip_object:
            zip_object.extract(output_filename)

        os.remove(zip_location)


    def get_dataset_2016(self):
        
        # retrieve file locations and data query string from config file
        self.config.read('config.cfg')
        query_string = self.config['querys']['QUERY_URL_2016']
        zip_location = self.config['files']['ZIP_LOCATION_2016']
        output_file = self.config['files']['OUTPUT_FILE_2016']

        try:
            # API request to download zip file containing census data
            print('downloading 2016 census data')
            response = requests.get(query_string, stream=True)
            response.raise_for_status() 

            # write returned data to zip location of interest
            with open(zip_location, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"File '{zip_location}' downloaded successfully.")

        except requests.exceptions.RequestException as e:
            print(f"Error during download: {e}")

        # Extract main datafile from zip folder to working directory
        with ZipFile(zip_location, 'r') as zip_object:
            zip_object.extract(output_file)
        
        os.remove(zip_location)



    def combine_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Method to implement cleaning and aggregations functions
        Returns: tuple of dataframes containing final output data
        """
        # Complete cleaning process for each vintage
        self._clean_data_2016()
        self._clean_data_2021()

        # generate output data at child and parent census tract levels
        long_form_data = self._generate_long_dataset().dropna()
        agg_data = self._generate_aggregate_dataset().dropna()
        return long_form_data, agg_data


    def _clean_data_2021(self):

        # ingest config
        self.config.read('config.cfg')
        output_file = self.config['files']['OUTPUT_FILE_2021']

        # read downloaded 2021 dataset
        data_2021_raw = pd.read_csv(output_file, encoding = 'latin1')

        # Filter and clean 
        data_2021_raw = data_2021_raw[data_2021_raw['GEO_LEVEL'] == 'Census tract']
        data_2021_raw['CHARACTERISTIC_NAME'] = data_2021_raw['CHARACTERISTIC_NAME'].apply(lambda x: x.lstrip())
        self.data_2021 = data_2021_raw[ast.literal_eval(self.config['columns']['COLUMNS_2021'])][(data_2021_raw['CHARACTERISTIC_ID'].isin(ast.literal_eval(self.config['characteristics']['CHAR_CODES']))) | (data_2021_raw['CHARACTERISTIC_NAME'].isin(ast.literal_eval(self.config['characteristics']['CHAR_NAMES'])))]
        self.data_2021['AGGREGATE_CT'] = self.data_2021['ALT_GEO_CODE'].astype(int)


    def _clean_data_2016(self):

        self.config.read('config.cfg')
        output_file = self.config['files']['OUTPUT_FILE_2016']
        data_2016_raw = pd.read_csv(output_file)
        data_2016_raw = data_2016_raw[~data_2016_raw['Dim: Sex (3): Member ID: [1]: Total - Sex'].isin(['x', 'F', '..'])]
        data_2016_raw['DIM: Profile of Census Tracts (2247)'] = data_2016_raw['DIM: Profile of Census Tracts (2247)'].map(lambda x: x.lstrip(), na_action = 'ignore')
        data_2016_raw = data_2016_raw[ast.literal_eval(self.config['columns']['COLUMNS_2016'])][(data_2016_raw['Member ID: Profile of Census Tracts (2247)'].isin(ast.literal_eval(self.config['characteristics']['CHAR_CODES']))) | (data_2016_raw['DIM: Profile of Census Tracts (2247)'].isin(ast.literal_eval(self.config['characteristics']['CHAR_NAMES'])))]
        data_2016_raw.columns = ['CENSUS_YEAR', 'ALT_GEO_CODE', 'CHARACTERISTIC_NAME', 'CHARACTERISTIC_ID', 'C1_COUNT_TOTAL']
        self.data_2016 = data_2016_raw[['CENSUS_YEAR', 'ALT_GEO_CODE', 'CHARACTERISTIC_ID', 'CHARACTERISTIC_NAME', 'C1_COUNT_TOTAL']]
        self.data_2016['C1_COUNT_TOTAL'] = self.data_2016['C1_COUNT_TOTAL'].astype(float)
        self.data_2016['AGGREGATE_CT'] = self.data_2016['ALT_GEO_CODE'].astype(int)
    

    def _generate_long_dataset(self):
        geos = self.data_2021['ALT_GEO_CODE'].unique()
        filtered_data_2016 = self.data_2016[self.data_2016['ALT_GEO_CODE'].isin(geos)]
        long_form_data = pd.concat([filtered_data_2016, self.data_2021])
        long_form_data['CHARACTERISTIC_NAME'] = long_form_data['CHARACTERISTIC_NAME'].apply(lambda x: x.split(",")[0] if x.split(",")[0] == 'Population' else x)
        long_form_data = long_form_data.pivot_table(index = ['ALT_GEO_CODE', 'CENSUS_YEAR', 'AGGREGATE_CT'], columns = ['CHARACTERISTIC_NAME'], values = 'C1_COUNT_TOTAL').reset_index()
        return long_form_data


    def _generate_aggregate_dataset(self):
        agg_geos = self.data_2021['AGGREGATE_CT'].unique()
        filtered_data_2016 = self.data_2016[self.data_2016['AGGREGATE_CT'].isin(agg_geos)]
        long_data = pd.concat([filtered_data_2016, self.data_2021])
        long_data['CHARACTERISTIC_NAME'] = long_data['CHARACTERISTIC_NAME'].apply(lambda x: x.split(",")[0] if x.split(",")[0] == 'Population' else x)
        long_data = long_data.pivot_table(index = ['ALT_GEO_CODE', 'CENSUS_YEAR', 'AGGREGATE_CT'], columns = ['CHARACTERISTIC_NAME'], values = 'C1_COUNT_TOTAL').reset_index()
        agg_data = long_data.drop(columns = 'ALT_GEO_CODE').groupby(['AGGREGATE_CT', 'CENSUS_YEAR']).median().reset_index()
        return agg_data


    # Extra method to create wide-form dataset
    ## not implemented in final solution
    def _generate_wide_dataset(self):
        self.data_2016['CHARACTERISTIC_NAME'] = self.data_2016['CHARACTERISTIC_NAME'].apply(lambda x: x+', 2016' if x != 'Population, 2016' else x)
        self.data_2021['CHARACTERISTIC_NAME'] = self.data_2021['CHARACTERISTIC_NAME'].apply(lambda x: x+', 2021' if x != 'Population, 2021' else x)
        pivoted_2021 = self.data_2021.pivot_table(index = ['ALT_GEO_CODE'], columns = ['CHARACTERISTIC_NAME'], values = 'C1_COUNT_TOTAL').reset_index()
        pivoted_2016 = self.data_2016.pivot_table(index = ['ALT_GEO_CODE'], columns = ['CHARACTERISTIC_NAME'], values = 'C1_COUNT_TOTAL').reset_index()
        wide_form_data = pd.merge(pivoted_2016, pivoted_2021, on = 'ALT_GEO_CODE', how = 'inner')
        return wide_form_data
    
    








