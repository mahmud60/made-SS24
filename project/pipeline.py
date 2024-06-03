import pandas as pd
import opendatasets as od
from sqlalchemy import create_engine

class Pipeline:
    
    def __init__(self):
        self.engine = create_engine('sqlite:///../data/ProjectDatabase.db')
    
    def pull_carbon_emission_data(self):
        carbon_emission = pd.read_csv('https://zenodo.org/records/10562476/files/GCB2023v43_MtCO2_flat.csv')
        carbon_emission = carbon_emission.drop(['ISO 3166-1 alpha-3','UN M49'], axis=1)

        cols = carbon_emission.columns.difference(['Country'])
        for column in cols:
            carbon_emission[column] = pd.to_numeric(carbon_emission[column], errors='coerce')
        carbon_emission.fillna(0, inplace=True)
        carbon_emission['Year'] = carbon_emission['Year'].astype(int)
        filtered_data = carbon_emission[carbon_emission['Year'] >= 1950]
        filtered_data.to_sql('emissions', con=self.engine, index=False, if_exists='replace')
    
    
    def pull_renewable_energy_data(self):

        # the user credentials are saved in the kaggle json file, please replace the kaggle json file with yours
        dataset = 'https://www.kaggle.com/datasets/hungenliao/global-sustainable-energy-from-1996-to-2016'
        od.download(dataset)

        renewable_energy = pd.read_csv('global-sustainable-energy-from-1996-to-2016/Sustainable.csv')
        
        renewable_energy = renewable_energy.drop(['Country Code', 'Time Code'], axis=1)
        renewable_energy = renewable_energy.rename(columns={'Time':'Year', 'Renewable energy consumption (TJ) [3.1_RE.CONSUMPTION]':'Renewable energy consumption'})
        renewable_energy = renewable_energy.iloc[:-2]
        renewable_energy = renewable_energy.dropna(subset=['Country Name'])
        cols = renewable_energy.columns.difference(['Country Name'])
        renewable_energy['Year'] = renewable_energy['Year'].astype(int)
        for column in cols:
            renewable_energy[column] = pd.to_numeric(renewable_energy[column], errors='coerce')
        renewable_energy.fillna(0, inplace=True)
        renewable_energy.to_sql('renewable', con=self.engine, index=False, if_exists='replace')
    
    def initialize_Pipeline(self):
        self.pull_carbon_emission_data()
        self.pull_renewable_energy_data()

# Create an instance of the Pipeline class
pipeline_instance = Pipeline()

# Run the pipeline
pipeline_instance.initialize_Pipeline()