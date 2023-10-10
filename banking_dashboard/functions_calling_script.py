from helper_fcns import *

# Ingest census tract data. User will need one of the csv files from the following urls downloaded in the same directory as this function to run it:
# Dallas -> https://data.census.gov/table/DECENNIALPL2020.P4?g=050XX00US48113$1400000&y=2020&d=DEC+Redistricting+Data+(PL+94-171)
# Collin -> https://data.census.gov/table/DECENNIALPL2020.P4?g=050XX00US48085$1400000&y=2020&d=DEC+Redistricting+Data+(PL+94-171)
# Tarrant -> https://data.census.gov/table/DECENNIALPL2020.P4?g=050XX00US48439$1400000&y=2020&d=DEC+Redistricting+Data+(PL+94-171)
census_df = census_data_ingester('DECENNIALPL2020.P4-2023-09-05T202447.csv')

# Ingest ffiec census data. User will need to have the files at the following urls downloaded in the same directory as this function to run it(THESE URLS WILL DOWNLOAD THE FILE WHEN PASTED IN BROWSER):
# 2022 Flat File -> https://www.ffiec.gov/Census/Census_Flat_Files/CensusFlatFile2022.zip
# File Definitions -> https://www.ffiec.gov/Census/Census_Flat_Files/FFIEC_Census_File_Definitions_26AUG22.xlsx
ffiec_data = ffiec_flat_file_extractor('CensusFlatFile2022.csv','FFIEC_Census_File_Definitions_26AUG22.xlsx')

# Ingest files from HMDA website. User will need to have files at the following urls downloaded in the same directory as these functions to run them(THESE URLS WILL DOWNLOAD THE FILE WHEN PASTED IN BROWSER):
# LAR -> https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2022/2022_public_lar_csv.zip
# TS -> https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2022/2022_public_ts_csv.zip
# Panel -> https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2022/2022_public_panel_csv.zip
# MSA/MD Description - > https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2022/2022_public_msamd_csv.zip
hmda_dict = hmda_data_ingester('a') # can be any string currently 


# Ingest cra data. User will need to have zip files at the following urls downloaded in in the same directory as these functions to run them(THESE URLS WILL DOWNLOAD THE FILE WHEN PASTED IN BROWSER):
# 2021 Agg Data -> https://www.ffiec.gov/cra/xls/21exp_aggr.zip
# 2021 Discl Data -> https://www.ffiec.gov/cra/xls/21exp_discl.zip
cra_dict = cra_data_ingester('t') # can be any string currently 
cra_dict_no_fips = cra_mapping_function(cra_dict)
fcc_fips_url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
fcc_fips = fcc_fips_mappings_getter(fcc_fips_url)
cra_dict_mapped_fips = state_county_fips_mapper(cra_dict_no_fips, fcc_fips)
final_cra_dict = thousands_adder(cra_dict_mapped_fips)

# Ingest fdic institutions and locations data. User will need to have files from the following urls downloaded in the same directory as these functions to run them (THESE URLS WILL DOWNLOAD THE FILE WHEN PASTED IN BROWSER):
# Institutions -> https://s3-us-gov-west-1.amazonaws.com/cg-2e5c99a6-e282-42bf-9844-35f5430338a5/downloads/institutions.csv
# Institution defs -> https://banks.data.fdic.gov/docs/institutions_definitions.csv
# locations -> https://s3-us-gov-west-1.amazonaws.com/cg-2e5c99a6-e282-42bf-9844-35f5430338a5/downloads/locations.csv
# location defs -> https://banks.data.fdic.gov/docs/locations_definitions.csv
replace_map_columns = changec_label_adder('institutions_definitions.csv')
fdic_institutions_df = fdic_institutions_ingester('institutions.csv', replace_map_columns)
fdic_locations_df = fdic_locations_mapper('locations_definitions.csv','locations.csv')