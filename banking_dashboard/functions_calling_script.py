from helper_fcns import *

# ingest census tract data 
census_data = census_data_ingester()

# ingest ffiec census data 
ffiec_data = ffiec_flat_file_extractor()

#ingest files from HDMA website
hdma_dict = hdma_helper_fcn()