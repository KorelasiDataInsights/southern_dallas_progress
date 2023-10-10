from helper_fcns import *

# ingest census tract data 
census_df = census_data_ingester('DECENNIALPL2020.P4-2023-09-05T202447.csv')

# ingest ffiec census data 
ffiec_data = ffiec_flat_file_extractor('a','b')

# ingest files from HMDA website
hmda_dict = hmda_data_ingester('a')
hmda_df = hmda_data_merger(hmda_dict)

# ingest cra data 
cra_dict = cra_data_ingester('t')
cra_dict_no_fips = cra_mapping_function(cra_dict)
fcc_fips_url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
fcc_fips = fcc_fips_mappings_getter(fcc_fips_url)
final_cra_dict = state_county_fips_mapper(cra_dict_no_fips, fcc_fips)


# ingest fdic institutions and locations data
replace_map_columns = changec_label_adder('institutions_definitions.csv')
fdic_institutions_df = fdic_institutions_ingester('institutions.csv', replace_map_columns)
fdic_locations_df = fdic_locations_mapper('locations_definitions.csv','locations.csv')