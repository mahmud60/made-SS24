import pandas as pd
import opendatasets as od
from sqlalchemy import create_engine
import requests

class Pipeline:
    
    def __init__(self):
        self.engine = create_engine('sqlite:///../data/ProjectDatabase.db')
    
    def pull_carbon_emission_data(self):
        try:
            # Attempt to read the CSV file from the URL
            dataset = pd.read_csv('https://zenodo.org/records/10562476/files/GCB2023v43_MtCO2_flat.csv')
            carbon_emission = pd.DataFrame(dataset)
            print("CSV file has been loaded successfully:")
            print(carbon_emission.head())  # Display the first few rows of the DataFrame
            carbon_emission = pd.DataFrame(dataset)
            carbon_emission = carbon_emission.drop(['ISO 3166-1 alpha-3','UN M49'], axis=1)

            cols = carbon_emission.columns.difference(['Country', 'Year'])
            for column in cols:
                carbon_emission[column] = pd.to_numeric(carbon_emission[column], errors='coerce')
            medians = carbon_emission.T.rolling(5, min_periods=0, closed='both').median()
            carbon_emission.fillna(medians, inplace=True)
            carbon_emission['Year'] = carbon_emission['Year'].astype(int)
            filtered_data = carbon_emission[carbon_emission['Year'] >= 1996]
            filtered_data.to_sql('emissions', con=self.engine, index=False, if_exists='replace')

        except pd.errors.EmptyDataError:
            print("No data: The file is empty.")
        except pd.errors.ParserError:
            print("Parse error: There might be an issue with the format of the file.")
        except Exception as e:
            # Catch other pandas read_csv errors not specified above
            print(f"An error occurred: {str(e)}")
        
    
    
    def pull_renewable_energy_data(self):
        try:
            # the user credentials are saved in the kaggle json file, please replace the kaggle json file with yours
            dataset = 'https://www.kaggle.com/datasets/hungenliao/global-sustainable-energy-from-1996-to-2016'
            od.download(dataset)

            dataset = pd.read_csv('global-sustainable-energy-from-1996-to-2016/Sustainable.csv')
            renewable_energy = pd.DataFrame(dataset)
            
            renewable_energy = renewable_energy.drop(['Country Code', 'Time Code', 'Access to Clean Fuels and Technologies for cooking (% of total population) [2.1_ACCESS.CFT.TOT]', 
                                                    'Access to electricity (% of rural population with access) [1.2_ACCESS.ELECTRICITY.RURAL]', 'Access to electricity (% of total population) [1.1_ACCESS.ELECTRICITY.TOT]', 
                                                    'Access to electricity (% of urban population with access) [1.3_ACCESS.ELECTRICITY.URBAN]', 'Energy intensity level of primary energy (MJ/2011 USD PPP) [6.1_PRIMARY.ENERGY.INTENSITY]',
                                                    'Renewable electricity output (GWh) [4.1.2_REN.ELECTRICITY.OUTPUT]', 'Renewable electricity share of total electricity output (%) [4.1_SHARE.RE.IN.ELECTRICITY]',
                                                    'Renewable energy share of TFEC (%) [2.1_SHARE.TOTAL.RE.IN.TFEC]', 'Total electricity output (GWh) [4.1.1_TOTAL.ELECTRICITY.OUTPUT]',
                                                    'Total final energy consumption (TFEC) (TJ) [1.1_TOTAL.FINAL.ENERGY.CONSUM]'], axis=1)
            renewable_energy = renewable_energy.rename(columns={'Time':'Year', 'Renewable energy consumption (TJ) [3.1_RE.CONSUMPTION]':'Renewable energy consumption'})
            renewable_energy = renewable_energy.iloc[:-2]
            renewable_energy = renewable_energy.dropna(subset=['Country Name'])
            cols = renewable_energy.columns.difference(['Country Name'])
            renewable_energy['Year'] = renewable_energy['Year'].astype(int)
            for column in cols:
                renewable_energy[column] = pd.to_numeric(renewable_energy[column], errors='coerce')
            renewable_energy.fillna(0, inplace=True)
            renewable_energy.to_sql('renewable', con=self.engine, index=False, if_exists='replace')

        except requests.exceptions.HTTPError as he:
            print(f"HTTP error occurred: {he}")  # Handle HTTP errors like 404, 403, etc.
        except requests.exceptions.ConnectionError as ce:
            print(f"Connection error occurred: {ce}")  # Handle networking issues
        except Exception as e:
            print(f"An unexpected error occurred: {e}")  # Catch-all for any other exceptions
    
    def initialize_Pipeline(self):
        self.pull_carbon_emission_data()
        self.pull_renewable_energy_data()

# Create an instance of the Pipeline class
pipeline_instance = Pipeline()

# Run the pipeline
pipeline_instance.initialize_Pipeline()