import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import data_processing as dp
from configparser import ConfigParser
import os


st.set_page_config(page_title = 'Census Data Harmonization', layout = 'wide')

if st.button("Load Data"):
    config = ConfigParser()
    config.read('config.cfg')
    proc = dp.DataBuilder()
    
    if not os.path.exists(config['files']['OUTPUT_FILE_2021']):
        with st.spinner("Downloading 2021 Census Data"):
            proc.get_dataset_2021()

    if not os.path.exists(config['files']['OUTPUT_FILE_2016']):
        with st.spinner("Downloading 2016 Census Data"):
            proc.get_dataset_2016()
    
    st.session_state['final_data_long'], st.session_state['final_data_agg'] = proc.combine_data()
    

if 'final_data_long' in st.session_state:
    config = ConfigParser()
    long_csv = st.session_state['final_data_long'].to_csv(index=False, ).encode('utf-8')

    st.download_button(
        label="Download CSV",
        data=long_csv,
        file_name="final_data_long.csv",
        mime="text/csv",
    )
if 'final_data_agg' in st.session_state:
    config = ConfigParser()
    agg_csv = st.session_state['final_data_agg'].to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download CSV",
        data=agg_csv,
        file_name="final_data_agg.csv",
        mime="text/csv",
    )

st.divider()

if 'final_data_agg' in st.session_state:
    st.header('Example Uses of Long Form Dataset')
    

    cont1, cont2 = st.columns(2)
    with cont1:
        metric = st.selectbox('Select Variable of Interest for Aggregate Comparison', options = st.session_state['final_data_long'].columns[3:], placeholder= 'Population')
        filtered_data = st.session_state['final_data_long'].groupby('CENSUS_YEAR')[metric].mean().reset_index()
        fig, ax = plt.subplots(figsize=(5, 3))
        sns.barplot(data = filtered_data, x = 'CENSUS_YEAR', y = metric, ax = ax)
        st.pyplot(fig)

        st.divider()

        
    with cont2:
        metric = st.selectbox('Select Variable of Interest for Granular Comparison', options = st.session_state['final_data_long'].columns[3:], placeholder= 'Population')
        ct = float(st.text_input('Input the Census Tract Geo Code for the Census Tract of Interest', value = '10003.02'))

        filtered_data = st.session_state['final_data_long'][st.session_state['final_data_long']['ALT_GEO_CODE'] == ct]
        if len(filtered_data) == 0:
            st.write('Census tract filter returned no data, please try a different census tract geo code')
        else:
            fig, ax = plt.subplots(figsize=(5, 3))
            sns.barplot(data = filtered_data, x = 'CENSUS_YEAR', y = metric, ax = ax)
            st.pyplot(fig)




