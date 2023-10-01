from helper_fcns import *

# ingest census tract data 
census_df = census_data_ingester()

# ingest ffiec census data 
ffiec_df = ffiec_flat_file_extractor()

#ingest files from HDMA website
hdma_dict = hdma_data_ingester()
hdma_df = hdma_data_merger(hdma_dict)


#ingest cra data 
cra_dict = cra_data_ingester(url:str)
cra_df_final = cra_mapping_function(cra_dict)
