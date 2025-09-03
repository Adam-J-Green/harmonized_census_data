# Aggregation of Harmonized Census Data 

The following commands can be run from the terminal to complete the data aggregation process:
## Install package requirements
```pip install -r requirements.txt```

## Run Data Aggregation process
```python -m gather_data.py```

Optional:
## Run Streamlit Application Locally
```streamlit run app.py```

## Functionality
All logic to download, clean and aggregate 2016 and 2021 Canadian Census data is contained in *data_processing.py*. This logic is applied in *gather_data.py* to save two output datasets to the working directory. 

Data file 1 (final_data_long.csv): contains 2016 and 2021 census data at the census tract level for the following characteristics: Total private dwellings, population density per square kilometre, average age of the population, average size of census families, number of children.

Data file 2: (final_data_agg.csv): contains 2016 and 2021 census data for aggregate parent census tracts for the following characteristics: Total private dwellings, population density per square kilometre, average age of the population, average size of census families, number of children

## Data Dictionary

AGGREGATE_CT: Parent census tract geo code
ALT_GEO_CODE: Chil census tract geo code 
CENSUS_YEAR: Census year of observations
Average age of the population: Average population age individuals in census tract, all age groups
Average size of census families: Average number of members per family in census tract
Children: Average number of children  
Population
Population density per square kilometre
Total private dwellings
