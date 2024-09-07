# the following are the helper functions that will be used to ingest the data from sources in Aappendix A

import requests
from bs4 import BeautifulSoup
# import wget 
import zipfile 
import json
import os
import pandas as pd
import numpy as np
import requests
import re
from datetime import datetime
import zipcodes
import census_geocoder as geocoder
from tqdm import tqdm # to display a progress bar
from validator_collection import validators
from census_geocoder.constants import CENSUS_API_URL
from backoff_utils import backoff
import requests
import csv
import time


# General  functions not specific to data source
def fmt_respondent_id(respondent_id_val):
  # Cast as string if necessary
  respondent_id_val = str(respondent_id_val)
  # Include as many leading zeroes as needed to get up to 10 characters
  return respondent_id_val.zfill(10)

def format_census_tract(tract_number):
    return '%07.2F'% tract_number

def state_abrevs_getter(ssa_url: str)->dict[str: str]:
    """Used to download and convert each state abbreviation and its full name into a dictionary which will be the output.
    Args: 
        url: social security administration website

    Returns:
        A dictionary containing the state abbreviation as the key and the full state name as the value
    """
    ssa_html = requests.get(ssa_url)
    ssa_soup = BeautifulSoup(ssa_html.content, 'html.parser')
    state_abbrevs = {}
    for state in ssa_soup.find_all('tr'):
        state_abbrevs[state.text.strip().split()[-1]] = ' '.join(state.text.strip().split()[:-1])
    return state_abbrevs

def fcc_fips_mappings_getter(url: str)->dict[str: dict[str: str]]:
    """Used to download and convert county and state codes from fcc website into a python dictionary.
    
    Args:
        url: fcc state and county codes website.
    
    Returns: 
        A dictionary containing fcc fips codes as keys and thier corresponding state or county name as values.     
    """
    fips_url = url
    fips_html = requests.get(fips_url)
    state_fips_dict = {}
    counties_fips_dict = {} 
    for row_num, area_and_fips in enumerate(str(fips_html.content).split('\\n')):
        if row_num in range(16,67):        
            state_fips_dict[''.join(re.findall(r'[0-9]+',area_and_fips))] = ' '.join(re.findall(r'[a-zA-Z]+',area_and_fips))
        if row_num in range(72,3267):
            counties_fips_dict[''.join(re.findall(r'[0-9]+',area_and_fips))] = ' '.join(re.findall(r'[a-zA-Z]+',area_and_fips))
    return {'fcc_states':state_fips_dict,'fcc_counties':counties_fips_dict}

def get_zip_codes(county_name: str, abbrev_state: str = "TX")->list[str]:
    """Used to find all unique zipcodes associates with a given county and state.

    Args:
        county_name: name of county you want to find the associated zip codes of. 
        abbrev_state: name of county you want to find the associated zip codes of.
    Returns: 
        A list of zipcodes associates with th provided county and state.
    """
    if " County" not in county_name:
     county_name = county_name + " County"

    try:
        zip_list = zipcodes.filter_by(county = county_name, state = abbrev_state)

        zips_raw = [i['zip_code'] for i in zip_list]
        zips_unique = pd.Series(zips_raw).drop_duplicates().tolist()
        zips_unique.sort()

        #print(f"{len(zips_unique)} unique zip codes found in {county_name}")
        return zips_unique
    
    except:
        return None

def county_to_countyzip_dict(county_to_search: list[str])->dict[str: set]:
    """Used to create a dictionary of counties with zipcodes. This only has to be run once so the later mapping runs faster.

    Args:
        county_to_search: county that will be returned with zip codes
    
    Returns: 
        A dictionary of counties associated with zip codes. 
    """
    county_zips_dct = {}
    for county_i in county_to_search:
        zips_for_county = set(get_zip_codes(county_name = county_i, abbrev_state = "TX"))
        county_zips_dct[county_i] = zips_for_county
    return county_zips_dct   

def zip_to_county_name(zpcode: str, zips_dct: dict[str: set])->str:
    """Used to return name of county associated with a given zip code.

    Args:
        zpcode: provided zipcode 
        zips_dct: a dictionary that has county names as keys and the associated zip codes in a list as the value.
    
    Returns:
        The name of the county associated with the zip code as a value.
    """
    if zpcode in zips_dct["Dallas"]:
        return "Dallas County"
    elif zpcode in zips_dct["Collin"]:
        return "Collin County"
    elif zpcode in zips_dct["Tarrant"]:
        return "Tarrant County"
    else: 
        return "Other"

# def get_census_geocode(address_str: str)->dict[str: str]:
#     """Takes in an address and returns the Census Tract, County and State associated with it if available.

#     Args: 
#         address_str: the address string that the returned Census Tract, County and State will be based on.

#     Returns:
#         A dictionary containing the Census Tract, County and State associated with the provided address if they are available.
#     """
#     try:
#       # Get census geocoding
#       geo_dict = geocoder.geography.from_address(address_str).__dict__
    
#       # # Get geographical details
#       geo = geo_dict['extensions']['result']['addressMatches'][0]['geographies']
    
#       # Get census tract(s)... hopefully there's only one?
#       # If more than 1, print a warning: 
#       #        print(f"{len(census_tracts)} census tracts found. Using first result")
#       # Similar for states and counties
    
#       census_tracts = [i['TRACT'] for i in geo['Census Tracts']]
#       counties = [i['BASENAME'] for i in geo['Counties']]
#       states = [i['BASENAME'] for i in geo['States']]    
    
#       return {
#           'state': states[0],
#           'county': counties[0],
#           'census_tract': census_tracts[0],
#       }
#     except:
#         return {
#           'state': None,
#           'county': None,
#           'census_tract': None,
#       }
    
def census_batch_lookup(filename: str, srch_level: str):

  DEFAULT_BENCHMARK = os.environ.get('CENSUS_GEOCODER_BENCHMARK', 'CURRENT')
  DEFAULT_VINTAGE = os.environ.get('CENSUS_GEOCODER_VINTAGE', 'CURRENT')
  DEFAULT_LAYERS = os.environ.get('CENSUS_GEOCODER_LAYERS', 'all')

  print("Validate file existence")
  file_ = validators.file_exists(filename, allow_empty = False)

  print("Get batch addresses")
  result_ = geocoder.geography._get_batch_addresses(file_ = file_,
                                          benchmark = DEFAULT_BENCHMARK,
                                          vintage = DEFAULT_VINTAGE,
                                          layers = DEFAULT_LAYERS)
  
  print("Get results that were non-failures")
  res_nonfail = [i for i in result_ if len(i) >3]
 
  print(f"{len(res_nonfail)} out of {len(result_)} succeeded")

  print("Extract geographic components")
  res2 = [geocoder.geography.from_csv_record(x) for x in res_nonfail]

  print("Extract components as dicts")
  out = [i.__dict__ for i in res2]

  print("Format result")
  out_df = pd.DataFrame(out)
  out_df['_latitude'] = [out_df['extensions'][k]['latitute'] for k in range(0,out_df.shape[0])]
  out_df.dropna(how='all', axis=1, inplace=True) 
  out_df[srch_level + ' identifier'] = [i[0] for i in res_nonfail]
  out_df[srch_level + ' street address'] = [i[1] for i in res_nonfail]
  out_df['Match result'] = [i[2] for i in res_nonfail]
  out_df['Match type'] = [i[3] for i in res_nonfail]

  print("Done")

  return out_df

# census tract helper function
def census_data_ingester(census_file_common_string: str)-> pd.core.frame.DataFrame:
    """"Takes in url and downloads csv from census tract website. The downloaded csv is then read in as a dataframe 
    and transformed into a format that can be used for join on future tract years where 'tract','county',and 'state'
    are the composite primary key.
        
    Args:
        file_name: The name of the downloaded csv file from the census tract website.
        
    Returns:
         A dataframe of the downloaded and transformed zip file
    """
    
    # code to ingest from url to be added
    #---
    
    # transform ingested data
    census_files = [os.path.join('data', i) for i in os.listdir('data/') if census_file_common_string in i]
    census_df_list = []
    for file_name in census_files:
        data = pd.read_csv(file_name)
        data = data.T
        data = data.reset_index()
        data.columns = data.iloc[0]
        data['test1'] = data['Label (Grouping)'].str.split(',').to_frame()
        data[['tract','county','state']] = pd.DataFrame(data['test1'].to_list(), columns = ['tract','county','state'])[['tract','county','state']]
        data = data.drop(data.index[0])
        data = data.drop(columns = ['test1'])#, axis = 1)
        data = data.drop(columns = ['Label (Grouping)'])#, axis = 1)
        data['tract'] = data['tract'].str.replace('Census Tract', '').str.strip().apply(float).apply(format_census_tract)
        data = data.set_index(['tract','county','state'])
        data.columns = list(data.columns.str.replace(u'\xa0', u' ').str.replace(':','').str.lstrip(' ')) # remove \xa0 Latin1 characters and ":" in column names
        data = data.replace('[^0-9.]', '', regex = True) # replace commas in entry values with nothing 
        data = data.apply(pd.to_numeric,downcast = 'float') #convert all count values to floats for later calculations 
        census_df_list.append(data)
    census_df = pd.concat(census_df_list).reset_index()   
    census_df = census_df.rename(columns = lambda x: x.strip()) 
    return census_df

# ffiec helper function
def ffiec_flat_file_extractor(data_folder: str, file: str, data_dict: str, ingest_all=False)->pd.core.frame.DataFrame:
    """Used to extract csv files from ffiec website and convert into pandas dataframe.
    
    Args:
        url: The url path to the zip file in the ffiec website.
    
    Returns: 
        A dataframe of the downloaded and transformed zip file
        
    Raises: 
        
    """
    # filename = wget.download(file_url)
    # zip_ref = zipfile.ZipFile(filename, 'r')
    # current_dir = os.getcwd()
    # unzipped = zip_ref.extractall(current_dir)
    data_dictionary = pd.read_excel(os.path.join(data_folder,data_dict), sheet_name = 'Data Dictionary')
    data_dictionary = data_dictionary[data_dictionary['Index']>=0]
    new_ffiec_cols = data_dictionary['Description']
    if ingest_all:
        data = pd.read_csv(os.path.join(data_folder,file), header = None)
    else:
        data = pd.read_csv(os.path.join(data_folder,file), header = None) #nrows = 8000,
    old_ffiec_cols = data.columns
    replacement_map = dict(zip(old_ffiec_cols,new_ffiec_cols))
    data.rename(columns = replacement_map, inplace=True)
    # replace values in columns with their definitions. Ex: 1 replaced with "principal city" in Principal city flag column
    data["Principal city flag. 0=not principal city, 1=principal city"] =\
    data["Principal city flag. 0=not principal city, 1=principal city"].map({0:"not principal city",1:"principal city"})
    data["Small county flag. T=tract record, S=small county, I=island area"] =\
    data["Small county flag. T=tract record, S=small county, I=island area"].map({"T":"tract record",
                                                                                  "S":"small county",
                                                                                  "I":"island area"})
    
    data["Split tract flag. N=tract number occurs within one MA, S=split between Mas"] =\
    data["Split tract flag. N=tract number occurs within one MA, S=split between Mas"].map({"N":"tract number occurs within one MA", 
                                                                                            "S":"split between Mas"})
    
    data["Demographic data flag. X=Total persons/population or median family income is 0, D=total persons/population and median family income are not 0, I=Island Area"] =\
    data["Demographic data flag. X=Total persons/population or median family income is 0, D=total persons/population and median family income are not 0, I=Island Area"]\
    .map({"X":"Total persons/population or median family income is 0",
          "D":"total persons/population and median family income are not 0",
          "I":"Island Area"})
    
    data["Urban/rural flag. U=urban, R=rural, M=mixed, I=Island Area"] =\
    data["Urban/rural flag. U=urban, R=rural, M=mixed, I=Island Area"].map({"U":"urban",
                                                                            "R":"rural",
                                                                            "M":"mixed",
                                                                            "I":"Island Area"})
    
    data["CRA poverty criteria. 'X' - Yes , ' ' (blank space) - No"] =\
    data["CRA poverty criteria. 'X' - Yes , ' ' (blank space) - No"].map({'X':"Yes"}).fillna("No")
    data["CRA unemployment criteria. 'X' - Yes , ' ' (blank space) - No"] =\
    data["CRA unemployment criteria. 'X' - Yes , ' ' (blank space) - No"].map({"X":"Yes"}).fillna("No")
    data["CRA distressed criteria. 'X' - Yes , ' ' (blank space) - No"] =\
    data["CRA distressed criteria. 'X' - Yes , ' ' (blank space) - No"].map({"X":"Yes"}).fillna("No")
    data["CRA remote rural (low density) criteria. 'X' -Yes, ' ' (blank space) - No"] =\
    data["CRA remote rural (low density) criteria. 'X' -Yes, ' ' (blank space) - No"].map({"X":"Yes"}).fillna("No")
    data["Previous year CRA distressed criteria. 'X' - Yes , ' ' (blank space) - No"] =\
    data["Previous year CRA distressed criteria. 'X' - Yes , ' ' (blank space) - No"].map({"X":"Yes"}).fillna("No")
    data["Previous year CRA underserved criterion. 'X' - Yes , ' ' (blank space) - No"] =\
    data["Previous year CRA underserved criterion. 'X' - Yes , ' ' (blank space) - No"].map({"X":"Yes"}).fillna("No")
    data["Meets at least one of current or previous year's CRA distressed/underserved tract criteria? 'X' - Yes, ' ' (blank space) - No"] =\
    data["Meets at least one of current or previous year's CRA distressed/underserved tract criteria? 'X' - Yes, ' ' (blank space) - No"]\
    .map({"X":"Yes"}).fillna("No")
    # rename columns
    data.rename(columns = {"Key field. HMDA/CRA collection year":"HMDA/CRA collection year",
                       "Principal city flag. 0=not principal city, 1=principal city":"Principal city flag",
                      "Small county flag. T=tract record, S=small county, I=island area":"Small county flag",
                      "Split tract flag. N=tract number occurs within one MA, S=split between Mas":"Split tract flag",
                      "Demographic data flag. X=Total persons/population or median family income is 0, D=total persons/population and median family income are not 0, I=Island Area":"Demographic data flag",
                      "Urban/rural flag. U=urban, R=rural, M=mixed, I=Island Area":"Urban/rural flag",
                      "CRA poverty criteria. 'X' - Yes , ' ' (blank space) - No":"CRA poverty criteria",
                      "CRA unemployment criteria. 'X' - Yes , ' ' (blank space) - No":"CRA unemployment criteria",
                      "CRA distressed criteria. 'X' - Yes , ' ' (blank space) - No":"CRA distressed criteria",
                      "CRA remote rural (low density) criteria. 'X' -Yes, ' ' (blank space) - No":"CRA remote rural (low density) criteria",
                      "Previous year CRA distressed criteria. 'X' - Yes , ' ' (blank space) - No":"Previous year CRA distressed criteria",
                      "Previous year CRA underserved criterion. 'X' - Yes , ' ' (blank space) - No":"Previous year CRA underserved criterion",
                      "Meets at least one of current or previous year's CRA distressed/underserved tract criteria? 'X' - Yes, ' ' (blank space) - No":"Meets at least one of current or previous year's CRA distressed/underserved tract criteria?",
                      "Key field. MSA/MD Code":"MSA/MD Code",
                      "Key field. FIPS state code":"FIPS state code",
                      "Key field. FIPS county code":"FIPS county code",
                      "Key field. Census tract. Implied decimal point.":"Census tract. Implied decimal point"}, inplace = True)
    # cast alphanumeric values to stings and numeric only values to floats
    alphanumeric_field_list = ["HMDA/CRA collection year",
                "MSA/MD Code",
                "FIPS state code",
                "FIPS county code",
                "Census tract. Implied decimal point",
                "Principal city flag",
                "Small county flag",
                "Split tract flag",
                "Demographic data flag",
                "Urban/rural flag",
                "CRA poverty criteria",
                "CRA unemployment criteria",
                "CRA distressed criteria",
                "CRA remote rural (low density) criteria",
                "Previous year CRA distressed criteria",
                "Previous year CRA underserved criterion",
                "Meets at least one of current or previous year's CRA distressed/underserved tract criteria?"]
    alphanum_to_str_dict = {an_field: str for an_field in alphanumeric_field_list} 
    data = data.astype(alphanum_to_str_dict) # casting aplhanumeric fields to strings
    numeric_field_list = list(data.loc[:,~data.columns.isin(alphanumeric_field_list)].columns)
    numeric_to_float_dict = {n_field:float for n_field in numeric_field_list} 
    data = data.astype(numeric_to_float_dict) # casting numeric fields to floats 
    data['Census tract. Implied decimal point'] = data['Census tract. Implied decimal point'].apply(int)/100
    data['Census tract. Implied decimal point'] = data['Census tract. Implied decimal point'].apply(format_census_tract) 
    data['FIPS state code'] = data['FIPS state code'].apply(lambda x: '0'+ x if len(x)<2 else x)
    data['FIPS county code'] = data['FIPS state code'] + data['FIPS county code'].apply(zero_adder)
    url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
    fips_dict = fcc_fips_mappings_getter(url)
    data['FIPS state code'] = data['FIPS state code'].map(fips_dict['fcc_states'])
    data['FIPS county code'] = data['FIPS county code'].map(fips_dict['fcc_counties'])
    data = data[data['FIPS state code'] == 'TEXAS']
    data = data[(data['FIPS county code'] == 'Tarrant County') | (data['FIPS county code'] == 'Collin County') | (data['FIPS county code'] == 'Dallas County')]
    data = data.rename(columns = {'Census tract. Implied decimal point':'Census tract', 'FIPS county code':'County','FIPS state code':'State'})
    data = data.rename(columns = lambda x: x.strip())
    return data


def clean_county_code(county_code, validate_tx = False):
    """Cleans and formats a county code string.
    
    Args:
        county_code (int): The county code to be cleaned.

    Returns:
        str: The cleaned and formatted county code.
    """
    raw_county_code_str = str(county_code)
    county_code_str = str(county_code)
    county_code_str = county_code_str.replace('.0', '') # , regex=True
    #county_code_str = county_code_str[:-3]
    #if len(county_code_str) < 2:
    #    county_code_str = '0' + county_code_str
    #county_code_str += county_code_str[-3:]
    
    if validate_tx and ('48085' in raw_county_code_str) | ('48113' in raw_county_code_str) | ('48439' in raw_county_code_str): 
        print(f'County code = converted from `{raw_county_code_str}` to `{county_code_str}`')   
    
    return county_code_str



def clean_census_tract(census_tract, validate_tx = False):
    """Cleans and formats a census tract string.

    Args:
        census_tract (str): The census tract to be cleaned.

    Returns:
        str: The cleaned and formatted census tract.
    """
    if not census_tract:
        return '0000NAN'
    census_tract_raw = str(census_tract)
    census_tract = census_tract_raw.replace('.0', '')
    census_tract = census_tract[-6:]
    census_tract = ''.join(census_tract[-6:-2] + '.' + census_tract[-2:])
    census_tract = census_tract.replace('.an', 'an')
    if validate_tx and ('48085' in census_tract_raw) | ('48113' in census_tract) | ('48439' in census_tract): 
        print(f'Census tract = converted from `{census_tract_raw}` to `{census_tract}`')
    return census_tract

# hmda helper function
def hmda_data_ingester(url: str,
                       data_folder: str = 'data',
                       lar_file: str = 'lar',
                       panel_file: str = 'panel',
                       ts_file: str = 'ts') -> dict[pd.core.frame.DataFrame]:
    
    """Used to read in all necessary .csv files from HMDA website and return a dictionary containing all of the read in
    files.
    
    Args: 
        url: url of HMDA page with zip file datasets on it. 
        data_folder: file path for data folder 
        
    Returns:
        A dictionary of dataframes. One for each ingested file from the HMDA website.
        
    Raises:
        TypeError: if url is not a string.
    """
    
    # NOTE: page associated with url uses javascript so http request made will need to match http request being made from
    # javascript(https://stackoverflow.com/questions/26393231/using-python-requests-with-javascript-pages). Additionally,
    # the extracted Loan/Application Records (LAR) csv is greater than 5 gigabytes so will working to find other method 
    # of storing file. When analyzing the LAR dataset, using the dask library will work specificlly dask.dataframe().

    # read in loan/application records as df
    #lar_df = pd.read_csv(os.path.join(data_folder, 'public_lar_csv.csv'),  nrows = 2000000) 
    # mapping values for columns in Loan/Application Records(LAR)
    # file_year = data_folder.split('\\')[1]
    file_year = os.path.basename(data_folder)
    counter = 0
    lar_df_full = pd.DataFrame()
    for lar_df_chunk in pd.read_csv(os.path.join(data_folder, lar_file), chunksize=50000):
        lar_df_chunk['conforming_loan_limit'] = lar_df_chunk['conforming_loan_limit'].map({"C (Conforming)":"Conforming",
                                                    "NC (Nonconforming)":"Nonconforming",
                                                    "U (Undetermined)":"Undetermined",
                                                    "NA (Not Applicable)":"Not Applicable"})
        
        lar_df_chunk['action_taken'] = lar_df_chunk['action_taken'].map({1:"Loan originated",
                                                            2:"Application approved but not accepted",
                                                            3:"Application denied",
                                                            4:"Application withdrawn by applicant",
                                                            5:"File closed for incompleteness",
                                                            6:"Purchased loan",
                                                            7:"Preapproval request denied",
                                                            8:"Preapproval request approved but not accepted"})

        lar_df_chunk['purchaser_type'] = lar_df_chunk['purchaser_type'].map({0:"Not applicable",
                                                                1:"Fannie Mae",
                                                                2:"Ginnie Mae",
                                                                3:"Freddie Mac",
                                                                4:"Farmer Mac",
                                                                5:"Private securitizer",
                                                                6:"Commercial bank, savings bank, or savings association",
                                                                71:"Credit union, mortgage company, or finance company",
                                                                72:"Life insurance company",
                                                                8:"Affiliate institution",
                                                                9:"Other type of purchase r"})
        
        lar_df_chunk['preapproval'] = lar_df_chunk['preapproval'].map({1:"Preapproval requested",
                                                        2:"Preapproval not requested"})

        
        lar_df_chunk['loan_type'] = lar_df_chunk['loan_type'].map({1:"Conventional (not insured or guaranteed by FHA, VA, RHS, or FSA)",
                                                    2:"Federal Housing Administration insured (FHA)",
                                                    3:"Veterans Affairs guaranteed (VA)",
                                                    4:"USDA Rural Housing Service or Farm Service Agency guaranteed (RHS or FSA)"})
        
        lar_df_chunk['loan_purpose'] = lar_df_chunk['loan_purpose'].map({1:"Home purchase",
                                                            2:"Home improvement",
                                                            31:"Refinancing",
                                                            32:"Cash-out refinancing",
                                                            4:"Other purpose",
                                                            5:"Not applicable"})

        lar_df_chunk['lien_status'] = lar_df_chunk['lien_status'].map({1:"Secured by a first lien",
                                                        2:"Secured by a subordinate lien"})  

        lar_df_chunk['reverse_mortgage'] = lar_df_chunk['reverse_mortgage'].map({1:"Reverse mortgage",
                                                                    2:"Not a reverse mortgage",
                                                                    1111:"Exempt"})  
        
        lar_df_chunk['open_end_line_of_credit'] = lar_df_chunk['open_end_line_of_credit'].map({1:"Open-end line of credit",
                                                                                2:"Not an open-end line of credit",
                                                                                1111:"Exempt"})  

        lar_df_chunk['business_or_commercial_purpose'] = lar_df_chunk['business_or_commercial_purpose'].map({1:"Primarily for a business or commercial purpose",
                                                                                                2:"Not primarily for a business or commercial purpose",
                                                                                                1111:"Exempt"})

        lar_df_chunk['hoepa_status'] = lar_df_chunk['hoepa_status'].map({1:"High-cost mortgage",
                                                            2:"Not a high-cost mortgage",
                                                            3:"Not applicable"})

        lar_df_chunk['negative_amortization'] = lar_df_chunk['negative_amortization'].map({1:"Negative amortization",
                                                                            2:"No negative amortization",
                                                                            1111:"Exempt"})

        lar_df_chunk['interest_only_payment'] = lar_df_chunk['interest_only_payment'].map({1:"Interest-only payments",
                                                                            2:"No interest-only payments",
                                                                            1111:"Exempt"})

        lar_df_chunk['balloon_payment'] = lar_df_chunk['balloon_payment'].map({1:"Balloon payment",
                                                                2:"No balloon payment",
                                                                1111:"Exempt"})

        lar_df_chunk['other_nonamortizing_features'] = lar_df_chunk['other_nonamortizing_features'].map({1:"Other non-fully amortizing features",
                                                                                            2:"No other non-fully amortizing features",
                                                                                            1111:"Exempt"})

        lar_df_chunk['construction_method'] = lar_df_chunk['construction_method'].map({1:"Site-built",
                                                                        2:"Manufactured home"})

        lar_df_chunk['occupancy_type'] = lar_df_chunk['occupancy_type'].map({1:"Principal residence",
                                                                2:"Second residence",
                                                                3:"Investment property"})

        lar_df_chunk['manufactured_home_secured_property_type'] = lar_df_chunk['manufactured_home_secured_property_type'].map({1:"Manufactured home and land",
                                                                                                                2:"Manufactured home and not land",
                                                                                                                3:"Not applicable",
                                                                                                                1111:"Exempt"})

        lar_df_chunk['manufactured_home_land_property_interest'] = lar_df_chunk['manufactured_home_land_property_interest'].map({1:"Direct ownership",
                                                                                                                    2:"Indirect ownership",
                                                                                                                    3:"Paid leasehold",
                                                                                                                    4:"Unpaid leasehold",
                                                                                                                    5:"Not applicable",
                                                                                                                    1111:"Exempt"})

        lar_df_chunk['applicant_credit_score_type'] = lar_df_chunk['applicant_credit_score_type'].map({1:"Equifax Beacon 5.0",
                                                                                        2:"Experian Fair Isaac",
                                                                                        3:"FICO Risk Score Classic 04",
                                                                                        4:"FICO Risk Score Classic 98",
                                                                                        5:"VantageScore 2.0",
                                                                                        6:"VantageScore 3.0",
                                                                                        7:"More than one credit scoring model",
                                                                                        8:"Other credit scoring model",
                                                                                        9:"Not applicable",
                                                                                        1111:"Exempt"})

        lar_df_chunk['co_applicant_credit_score_type'] = lar_df_chunk['co_applicant_credit_score_type'].map({1:"Equifax Beacon 5.0",
                                                                                                2:"Experian Fair Isaac",
                                                                                                3:"FICO Risk Score Classic 04",
                                                                                                4:"FICO Risk Score Classic 98",
                                                                                                5:"VantageScore 2.0",
                                                                                                6:"VantageScore 3.0",
                                                                                                7:"More than one credit scoring model",
                                                                                                8:"Other credit scoring model",
                                                                                                9:"Not applicable",
                                                                                                10:"No co-applicant",
                                                                                                1111:"Exempt"})

        lar_df_chunk['applicant_ethnicity_1'] = lar_df_chunk['applicant_ethnicity_1'].map({1:"Hispanic or Latino",
                                                                            11:"Mexican",
                                                                            12:"Puerto Rican",
                                                                            13:"Cuban",
                                                                            14:"Other Hispanic or Latino",
                                                                            2:"Not Hispanic or Latino",
                                                                            3:"Information not provided by applicant in mail, internet, or telephone application",
                                                                            4:"Not applicable"})

        lar_df_chunk['applicant_ethnicity_2'] = lar_df_chunk['applicant_ethnicity_2'].map({1:"Hispanic or Latino",
                                                                                11:"Mexican",
                                                                                12:"Puerto Rican",
                                                                                13:"Cuban",
                                                                                14:"Other Hispanic or Latino",
                                                                                2:"Not Hispanic or Latino"})

        lar_df_chunk['applicant_ethnicity_3'] = lar_df_chunk['applicant_ethnicity_3'].map({1:"Hispanic or Latino",
                                                                            11:"Mexican",
                                                                            12:"Puerto Rican",
                                                                            13:"Cuban",
                                                                            14:"Other Hispanic or Latino",
                                                                            2:"Not Hispanic or Latino"})

        lar_df_chunk['applicant_ethnicity_4'] = lar_df_chunk['applicant_ethnicity_4'].map({1:"Hispanic or Latino",
                                                                                11:"Mexican",
                                                                                12:"Puerto Rican",
                                                                                13:"Cuban",
                                                                                14:"Other Hispanic or Latino",
                                                                                2:"Not Hispanic or Latino"})

        lar_df_chunk['applicant_ethnicity_5'] = lar_df_chunk['applicant_ethnicity_5'].map({1:"Hispanic or Latino",
                                                                                11:"Mexican",
                                                                                12:"Puerto Rican",
                                                                                13:"Cuban",
                                                                                14:"Other Hispanic or Latino",
                                                                                2:"Not Hispanic or Latino"})

        lar_df_chunk['co_applicant_ethnicity_1'] = lar_df_chunk['co_applicant_ethnicity_1'].map({1:"Hispanic or Latino",
                                                                                    11:"Mexican",
                                                                                    12:"Puerto Rican",
                                                                                    13:"Cuban",
                                                                                    14:"Other Hispanic or Latino",
                                                                                    2:"Not Hispanic or Latino",
                                                                                    3:"Information not provided by applicant in mail, internet, or telephone application",
                                                                                    4:"Not applicable",
                                                                                    5:"No co-applicant"})

        lar_df_chunk['co_applicant_ethnicity_2'] = lar_df_chunk['co_applicant_ethnicity_2'].map({1:"Hispanic or Latino",
                                                                                    11:"Mexican",
                                                                                    12:"Puerto Rican",
                                                                                    13:"Cuban",
                                                                                    14:"Other Hispanic or Latino",
                                                                                    2:"Not Hispanic or Latino"})

        lar_df_chunk['co_applicant_ethnicity_3'] = lar_df_chunk['co_applicant_ethnicity_3'].map({1:"Hispanic or Latino",
                                                                                    11:"Mexican",
                                                                                    12:"Puerto Rican",
                                                                                    13:"Cuban",
                                                                                    14:"Other Hispanic or Latino",
                                                                                    2:"Not Hispanic or Latino"})

        lar_df_chunk['co_applicant_ethnicity_4'] = lar_df_chunk['co_applicant_ethnicity_4'].map({1:"Hispanic or Latino",
                                                                                    11:"Mexican",
                                                                                    12:"Puerto Rican",
                                                                                    13:"Cuban",
                                                                                    14:"Other Hispanic or Latino",
                                                                                    2:"Not Hispanic or Latino"})

        lar_df_chunk['co_applicant_ethnicity_5'] = lar_df_chunk['co_applicant_ethnicity_5'].map({1:"Hispanic or Latino",
                                                                                    11:"Mexican",
                                                                                    12:"Puerto Rican",
                                                                                    13:"Cuban",
                                                                                    14:"Other Hispanic or Latino",
                                                                                    2:"Not Hispanic or Latino"})

        lar_df_chunk['applicant_ethnicity_observed'] = lar_df_chunk['applicant_ethnicity_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                            2:"Not collected on the basis of visual observation or surname",
                                                                                            3:"Not applicable"})

        lar_df_chunk['co_applicant_ethnicity_observed'] = lar_df_chunk['co_applicant_ethnicity_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                                2:"Not collected on the basis of visual observation or surname",
                                                                                                3:"Not applicable",
                                                                                                4:"No co-applicant"})

        lar_df_chunk['applicant_race_1'] = lar_df_chunk['applicant_race_1'].map({1:"American Indian or Alaska Native",
                                                                    2:"Asian",
                                                                    21:"Asian Indian",
                                                                    22:"Chinese",
                                                                    23:"Filipino",
                                                                    24:"Japanese",
                                                                    25:"Korean",
                                                                    26:"Vietnamese",
                                                                    27:"Other Asian",
                                                                    3:"Black or African American",
                                                                    4:"Native Hawaiian or Other Pacific Islander",
                                                                    41:"Native Hawaiian",
                                                                    42:"Guamanian or Chamorro",
                                                                    43:"Samoan",
                                                                    44:"Other Pacific Islander",
                                                                    5:"White",
                                                                    6:"Information not provided by applicant in mail, internet, or telephone application",
                                                                    7:"Not applicable"})
        
        lar_df_chunk['applicant_race_2'] = lar_df_chunk['applicant_race_2'].map({1:"American Indian or Alaska Native",
                                                                    2:"Asian",
                                                                    21:"Asian Indian",
                                                                    22:"Chinese",
                                                                    23:"Filipino",
                                                                    24:"Japanese",
                                                                    25:"Korean",
                                                                    26:"Vietnamese",
                                                                    27:"Other Asian",
                                                                    3:"Black or African American",
                                                                    4:"Native Hawaiian or Other Pacific Islander",
                                                                    41:"Native Hawaiian",
                                                                    42:"Guamanian or Chamorro",
                                                                    43:"Samoan",
                                                                    44:"Other Pacific Islander",
                                                                    5:"White"})
        
        lar_df_chunk['applicant_race_3'] = lar_df_chunk['applicant_race_3'].map({1:"American Indian or Alaska Native",
                                                                    2:"Asian",
                                                                    21:"Asian Indian",
                                                                    22:"Chinese",
                                                                    23:"Filipino",
                                                                    24:"Japanese",
                                                                    25:"Korean",
                                                                    26:"Vietnamese",
                                                                    27:"Other Asian",
                                                                    3:"Black or African American",
                                                                    4:"Native Hawaiian or Other Pacific Islander",
                                                                    41:"Native Hawaiian",
                                                                    42:"Guamanian or Chamorro",
                                                                    43:"Samoan",
                                                                    44:"Other Pacific Islander",
                                                                    5:"White"})
        
        lar_df_chunk['applicant_race_4'] = lar_df_chunk['applicant_race_4'].map({1:"American Indian or Alaska Native",
                                                                    2:"Asian",
                                                                    21:"Asian Indian",
                                                                    22:"Chinese",
                                                                    23:"Filipino",
                                                                    24:"Japanese",
                                                                    25:"Korean",
                                                                    26:"Vietnamese",
                                                                    27:"Other Asian",
                                                                    3:"Black or African American",
                                                                    4:"Native Hawaiian or Other Pacific Islander",
                                                                    41:"Native Hawaiian",
                                                                    42:"Guamanian or Chamorro",
                                                                    43:"Samoan",
                                                                    44:"Other Pacific Islander",
                                                                    5:"White"})
        
        lar_df_chunk['applicant_race_5'] = lar_df_chunk['applicant_race_5'].map({1:"American Indian or Alaska Native",
                                                                    2:"Asian",
                                                                    21:"Asian Indian",
                                                                    22:"Chinese",
                                                                    23:"Filipino",
                                                                    24:"Japanese",
                                                                    25:"Korean",
                                                                    26:"Vietnamese",
                                                                    27:"Other Asian",
                                                                    3:"Black or African American",
                                                                    4:"Native Hawaiian or Other Pacific Islander",
                                                                    41:"Native Hawaiian",
                                                                    42:"Guamanian or Chamorro",
                                                                    43:"Samoan",
                                                                    44:"Other Pacific Islander",
                                                                    5:"White"})

        lar_df_chunk['co_applicant_race_1'] = lar_df_chunk['co_applicant_race_1'].map({1:"American Indian or Alaska Native",
                                                                        2:"Asian",
                                                                        21:"Asian Indian",
                                                                        22:"Chinese",
                                                                        23:"Filipino",
                                                                        24:"Japanese",
                                                                        25:"Korean",
                                                                        26:"Vietnamese",
                                                                        27:"Other Asian",
                                                                        3:"Black or African American",
                                                                        4:"Native Hawaiian or Other Pacific Islander",
                                                                        41:"Native Hawaiian",
                                                                        42:"Guamanian or Chamorro",
                                                                        43:"Samoan",
                                                                        44:"Other Pacific Islander",
                                                                        5:"White",
                                                                        6:"Information not provided by applicant in mail, internet, or telephone application",
                                                                        7:"Not applicable",
                                                                        8:"No co-applicant"})

        lar_df_chunk['co_applicant_race_2'] = lar_df_chunk['co_applicant_race_2'].map({1:"American Indian or Alaska Native",
                                                                        2:"Asian",
                                                                        21:"Asian Indian",
                                                                        22:"Chinese",
                                                                        23:"Filipino",
                                                                        24:"Japanese",
                                                                        25:"Korean",
                                                                        26:"Vietnamese",
                                                                        27:"Other Asian",
                                                                        3:"Black or African American",
                                                                        4:"Native Hawaiian or Other Pacific Islander",
                                                                        41:"Native Hawaiian",
                                                                        42:"Guamanian or Chamorro",
                                                                        43:"Samoan",
                                                                        44:"Other Pacific Islander",
                                                                        5:"White"})

        lar_df_chunk['co_applicant_race_3'] = lar_df_chunk['co_applicant_race_3'].map({1:"American Indian or Alaska Native",
                                                                        2:"Asian",
                                                                        21:"Asian Indian",
                                                                        22:"Chinese",
                                                                        23:"Filipino",
                                                                        24:"Japanese",
                                                                        25:"Korean",
                                                                        26:"Vietnamese",
                                                                        27:"Other Asian",
                                                                        3:"Black or African American",
                                                                        4:"Native Hawaiian or Other Pacific Islander",
                                                                        41:"Native Hawaiian",
                                                                        42:"Guamanian or Chamorro",
                                                                        43:"Samoan",
                                                                        44:"Other Pacific Islander",
                                                                        5:"White"})

        lar_df_chunk['co_applicant_race_4'] = lar_df_chunk['co_applicant_race_4'].map({1:"American Indian or Alaska Native",
                                                                        2:"Asian",
                                                                        21:"Asian Indian",
                                                                        22:"Chinese",
                                                                        23:"Filipino",
                                                                        24:"Japanese",
                                                                        25:"Korean",
                                                                        26:"Vietnamese",
                                                                        27:"Other Asian",
                                                                        3:"Black or African American",
                                                                        4:"Native Hawaiian or Other Pacific Islander",
                                                                        41:"Native Hawaiian",
                                                                        42:"Guamanian or Chamorro",
                                                                        43:"Samoan",
                                                                        44:"Other Pacific Islander",
                                                                        5:"White"})

        lar_df_chunk['co_applicant_race_5'] = lar_df_chunk['co_applicant_race_5'].map({1:"American Indian or Alaska Native",
                                                                        2:"Asian",
                                                                        21:"Asian Indian",
                                                                        22:"Chinese",
                                                                        23:"Filipino",
                                                                        24:"Japanese",
                                                                        25:"Korean",
                                                                        26:"Vietnamese",
                                                                        27:"Other Asian",
                                                                        3:"Black or African American",
                                                                        4:"Native Hawaiian or Other Pacific Islander",
                                                                        41:"Native Hawaiian",
                                                                        42:"Guamanian or Chamorro",
                                                                        43:"Samoan",
                                                                        44:"Other Pacific Islander",
                                                                        5:"White"})

        lar_df_chunk['applicant_race_observed'] = lar_df_chunk['applicant_race_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                2:"Not collected on the basis of visual observation or surname",
                                                                                3:"Not applicable"})

        lar_df_chunk['co_applicant_race_observed'] = lar_df_chunk['co_applicant_race_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                        2:"Not collected on the basis of visual observation or surname",
                                                                                        3:"Not applicable",
                                                                                        4:"No co-applicant"})

        lar_df_chunk['applicant_sex'] = lar_df_chunk['applicant_sex'].map({1:"Male",
                                                            2:"Female",
                                                            3:"Information not provided by applicant in mail, internet, or telephone application",
                                                            4:"Not applicable",
                                                            6:"Applicant selected both male and female"})

        lar_df_chunk['co_applicant_sex'] = lar_df_chunk['co_applicant_sex'].map({1:"Male",
                                                                    2:"Female",
                                                                    3:"Information not provided by applicant in mail, internet, or telephone application",
                                                                    4:"Not applicable",
                                                                    5:"No co-applicant",
                                                                    6:"Co-applicant selected both male and female"})

        lar_df_chunk['applicant_sex_observed'] = lar_df_chunk['applicant_sex_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                2:"Not collected on the basis of visual observation or surname",
                                                                                3:"Not applicable"})

        lar_df_chunk['co_applicant_sex_observed'] = lar_df_chunk['co_applicant_sex_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                    2:"Not collected on the basis of visual observation or surname",
                                                                                    3:"Not applicable",
                                                                                    4:"No co-applicant"})

        lar_df_chunk['submission_of_application'] = lar_df_chunk['submission_of_application'].map({1:"Submitted directly to your institution",
                                                                                    2:"Not submitted directly to your institution",
                                                                                    3:"Not applicable",
                                                                                    1111:"Exempt"})

        lar_df_chunk['initially_payable_to_institution'] = lar_df_chunk['initially_payable_to_institution'].map({1:"Initially payable to your institution",
                                                                                                    2:"Not initially payable to your institution",
                                                                                                    3:"Not applicable",
                                                                                                    1111:"Exempt"})
        lar_df_chunk['aus_1'] = lar_df_chunk['aus_1'].map({1:"Desktop Underwriter (DU)",
                                            2:"Loan Prospector (LP) or Loan Product Advisor",
                                            3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                            4:"Guaranteed Underwriting System (GUS)",
                                            5:"Other",
                                            6:"Not applicable",
                                            7:"Internal Proprietary System",
                                            1111:"Exempt"})

        lar_df_chunk['aus_2'] = lar_df_chunk['aus_2'].map({1:"Desktop Underwriter (DU)",
                                        2:"Loan Prospector (LP) or Loan Product Advisor",
                                        3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                        4:"Guaranteed Underwriting System (GUS)",
                                        5:"Other",
                                        7:"Internal Proprietary System"})

        lar_df_chunk['aus_3'] = lar_df_chunk['aus_3'].map({1:"Desktop Underwriter (DU)",
                                        2:"Loan Prospector (LP) or Loan Product Advisor",
                                        3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                        4:"Guaranteed Underwriting System (GUS)",
                                        7:"Internal Proprietary System"})

        lar_df_chunk['aus-4'] = lar_df_chunk['aus_4'].map({1:"Desktop Underwriter (DU)",
                                        2:"Loan Prospector (LP) or Loan Product Advisor",
                                        3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                        4:"Guaranteed Underwriting System (GUS)",
                                        7:"Internal Proprietary System"})

        lar_df_chunk['aus_5'] = lar_df_chunk['aus_5'].map({1:"Desktop Underwriter (DU)",
                                        2:"Loan Prospector (LP) or Loan Product Advisor",
                                        3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                        4:"Guaranteed Underwriting System (GUS)",
                                        7:"Internal Proprietary System"})

        lar_df_chunk['denial_reason_1'] = lar_df_chunk['denial_reason_1'].map({1:"Debt-to-income ratio",
                                                                2:"Employment history",
                                                                3:"Credit history",
                                                                4:"Collateral",
                                                                5:"Insufficient cash (downpayment, closing costs)",
                                                                6:"Unverifiable information",
                                                                7:"Credit application incomplete",
                                                                8:"Mortgage insurance denied",
                                                                9:"Other",
                                                                10:"Not applicable"})

        lar_df_chunk['denial_reason_2'] = lar_df_chunk['denial_reason_2'].map({1:"Debt-to-income ratio",
                                                                2:"Employment history",
                                                                3:"Credit history",
                                                                4:"Collateral",
                                                                5:"Insufficient cash (downpayment, closing costs)",
                                                                6:"Unverifiable information",
                                                                7:"Credit application incomplete",
                                                                8:"Mortgage insurance denied",
                                                                9:"Other"})

        lar_df_chunk['denial_reason_3'] = lar_df_chunk['denial_reason_3'].map({1:"Debt-to-income ratio",
                                                                2:"Employment history",
                                                                3:"Credit history",
                                                                4:"Collateral",
                                                                5:"Insufficient cash (downpayment, closing costs)",
                                                                6:"Unverifiable information",
                                                                7:"Credit application incomplete",
                                                                8:"Mortgage insurance denied",
                                                                9:"Other"})

        lar_df_chunk['denial_reason_4'] = lar_df_chunk['denial_reason_4'].map({1:"Debt-to-income ratio",
                                                                2:"Employment history",
                                                                3:"Credit history",
                                                                4:"Collateral",
                                                                5:"Insufficient cash (downpayment, closing costs)",
                                                                6:"Unverifiable information",
                                                                7:"Credit application incomplete",
                                                                8:"Mortgage insurance denied",
                                                                9:"Other"})
        
        
        # Cleaning and formatting county code
        #lar_df_chunk['county_code'] = lar_df_chunk['county_code'].apply(str).str.replace('.0','').str[:-3].apply(lambda x: '0'+ x if len(x)<2 else x) + lar_df_chunk['county_code'].apply(str).str.replace('.0','').str[-3:]
        #lar_df_chunk['county_code'] = lar_df_chunk['county_code'].apply(str).str.replace('.0', '', regex=True).str[:-3].apply(lambda x: '0' + x if len(x) < 2 else x) + lar_df_chunk['county_code'].apply(str).str.replace('.0', '', regex=True).str[-3:]
        #global k_county_code
        #k_county_code = 0
        lar_df_chunk['county_code'] = lar_df_chunk['county_code'].apply(clean_county_code)
        
        # subset for texas and counties of interest
        # Collin County = 48085, Dallas County = 48113, Tarrant County = 48439
        lar_df_chunk = lar_df_chunk[lar_df_chunk['state_code'] == 'TX']
        lar_df_chunk = lar_df_chunk[(lar_df_chunk['county_code'] == '48439') | (lar_df_chunk['county_code'] == '48085') | (lar_df_chunk['county_code'] == '48113')]
        
        # concatenate chunks 
        lar_df_full = pd.concat([lar_df_full, lar_df_chunk])
        counter += 1
        print(str(counter) + "/??")

    # map in full state names
    ssa_url = 'https://www.ssa.gov/international/coc-docs/states.html'
    state_abbrev_map = state_abrevs_getter(ssa_url)
    lar_df_full['state_code'] = lar_df_full['state_code'].map(state_abbrev_map)

    # map in county names 
    #lar_df_chunk['county_code'] = lar_df_chunk['county_code'].apply(str).str.replace('.0','').str[:-3].apply(lambda x: '0'+ x if len(x)<2 else x) + lar_df_chunk['county_code'].apply(str).str.replace('.0','').str[-3:]
    url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
    fips_dict = fcc_fips_mappings_getter(url)
    lar_df_full['county_code'] = lar_df_full['county_code'].map(fips_dict['fcc_counties'])

    # recast data types
    lar_df_full = lar_df_full.astype({"derived_msa_md": str, "census_tract": str})

    # reformat the census tract column and subset for records in TEXAS and counties of interest and  stipping whitespace from column names
    lar_df_full['census_tract_full'] = lar_df_full['census_tract']
    #lar_df_full['census_tract'] = lar_df_full['census_tract'].str.replace('.0','').str[-6:].apply(lambda x: ''.join(x[-6:-2]+'.'+x[-2:])).str.replace('.an', 'an')
    lar_df_full['census_tract'] = lar_df_full['census_tract'].apply(clean_census_tract)
    #lar_df_full = lar_df_full[lar_df_full['state_code'] == 'TEXAS']
    #lar_df_full = lar_df_full[(lar_df_full['county_code'] == 'Tarrant County') | (lar_df_full['county_code'] == 'Collin County') | (lar_df_full['county_code'] == 'Dallas County')]
    lar_df_full = lar_df_full.rename(columns = lambda x: x.strip())
        
    # read in transmittal sheet records as df
    ts_df = pd.read_csv(os.path.join(data_folder, ts_file))
    
    # replacing values of "agency_code" with actual string fields in transmittal sheet dataset and removing whitespace from column names
    ts_df['agency_code'] = ts_df['agency_code'].map({1:"Office of the Comptroller of the Currency",
                                                     2:"Federal Reserve System",
                                                     3:"Federal Deposit Insurance Corporation",
                                                     5:"National Credit Union Administration",
                                                     7:"Department of Housing and Urban Development",
                                                     9:"Consumer Financial Protection Bureau"})
    ts_df = ts_df.rename(columns = lambda x: x.strip())    

    # read in reporter panel data as df
    panel_df = pd.read_csv(os.path.join(data_folder, panel_file), na_values = [-1]) # -1 is being encoded for NULL so I am replacing 
                                                                          # -1 with NaN. No description in data dictionary for 
                                                                          # field called "upper"
            
    # replacing values of "agency_code" with actual string fields
    panel_df['agency_code'] = panel_df['agency_code'].map({1:"Office of the Comptroller of the Currency",
                                                           2:"Federal Reserve System",
                                                           3:"Federal Deposit Insurance Corporation",
                                                           5:"National Credit Union Administration",
                                                           7:"Department of Housing and Urban Development",
                                                           9:"Consumer Financial Protection Bureau"})

    # replacing values of "other_lender_code" with actual string fields
    panel_df['other_lender_code'] = panel_df['other_lender_code'].map({0:"Depository Institution",
                                                                       1:"MBS of state member bank",
                                                                       2:"MBS of bank holding company",
                                                                       3:"Independent mortgage banking subsidiary",
                                                                       5:"Affiliate of a depository institution"}) 
    # renaming upper field to lei and removing whitespace from column names 
    panel_df.rename(columns = {'upper':'lei'}, inplace = True)
    panel_df = panel_df.rename(columns = lambda x: x.strip())

    # read in metropolitan statistical area and metropolitan division data as df
    # msamd_df = pd.read_csv(os.path.join('data', 'public_msamd_csv.csv')) # nothing written in data dictionary saying 99999 is na but it does
    #                                                     # not look like a legitamate msa_md code

    # recast data types and removing whitespace from column names 
    # msamd_df = msamd_df.astype({"msa_md": str})
    # msamd_df = msamd_df.rename(columns = lambda x: x.strip())    
        
    # arid_2017 = pd.read_csv('arid2017_to_lei_xref_csv.csv') # not using for the moment because not joining in previous 
                                                              # years so do not need to use                                                         
        
    hmda_dict = {"lar_df":lar_df_full,"ts_df":ts_df, "panel_df":panel_df}#, "msamd_df":msamd_df}
   
    return hmda_dict

# cra helper function      
def cra_data_ingester(file: str, data_folder: str = 'data', file_lst: list = []) -> dict[str: pd.core.frame.DataFrame]:
    """Used to read in cra .dat fwf files from directory(both agg and discl files need to be unzipped in directory where this function is being run from).
    
    Args:
        file: not currently used
    
    Returns: 
        A dictionary of dataframes     
    """
    #url = 'https://www.ffiec.gov/cra/xls/21exp_aggr.zip'
    #r = requests.get(url, allow_redirects = True)
    #open('21exp_aggr.zip','wb').write(r.content)
    #zip_ref = zipfile.ZipFile('21exp_aggr.zip', 'r') #zipfile not zip file error

    #def fixed width file mappings
    transmittal_fields = ["Respondent ID", "Agency Code","Activity Year","Respondent Name","Respondent Address","Respondent City",
                      "Respondent State","Respondent Zip Code", "Tax ID", "ID_RSSD", "Assets"]

    transmittal_widths = [10,1,4,30,40,25,2,10,10,10,10]



    a_1_1_fields  = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Census Tract", 
    "Split County Indicator", "Population Classification", "Income Group Total", "Report Level",
    "Number of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000", 
    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000",
    "Number of Small Business Loans Originated with Loan Amount at Origination > 100,000 and < or = to $250,000",
    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $100,000 and < or = to $250,000",
    "Number of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000", 
    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000" ,
    "Number of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million",
    "Total Loan Amount of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    a_1_1_widths = [5,4,1,1,2,3,5,7,1,1,3,3,10,10,10,10,10,10,10,10,29]
    
    
    
    a_1_1a_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Respondent ID", "Agency Code",
                     "Number of Lenders", "Report Level", "Number of Small Business Loans", "Total Loan Amount of Small Business Loans",
                     "Number of loans to Small Businesses with Gross Annual Revenues < or = to $1 million",
                     "Total Loan Amount of loans to Small Businesses with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    a_1_1a_widths = [5,4,1,1,2,3,5,10,1,5,3,10,10,10,10,65]
    
    a_1_2_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Census Tract", "Split County Indicator", 
    "Population Classification", "Income Group Total", "Report Level", 
    "Number of Small Business Loans Purchased with Loan Amount at Origination < or = to $100,000",
    "Total Loan Amount of Small Business Loans Purchased with Loan Amount at Origination < or = to $100,000", 
    "Number of Small Business Loans Purchased with Loan Amount at Origination > 100,000 and < or = to $250,000", 
    "Total Loan Amount of Small Business Loans Purchased with Loan Amount at Origination > $100,000 and < or = to $250,000", 
    "Number of Small Business Loans Purchased with Loan Amount at Origination > $250,000 and < or = to $1,000,000", 
    "Total Loan Amount of Small Business Loans Purchased with Loan Amount at Origination > $250,000 and < or = to $1,000,000", 
    "Number of Small Business Loans Purchased with Gross Annual Revenues < or = to $1 million", 
    "Total Loan Amount of Small Business Loans Purchased with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    
    a_1_2_widths = [5,4,1,1,2,3,5,7,1,1,3,3,10,10,10,10,10,10,10,10,29]
    
    
    a_1_2a_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Respondent ID",
    "Agency Code", "Number of Lenders", "Report Level", "Number of Small Business Loans", "Total Loan Amount of Small Business Loans", 
    "Number of loans to Small Businesses with Gross Annual Revenues < or = to $1 million",
    "Total Loan Amount of loans to Small Businesses with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    a_1_2a_widths = [5,4,1,1,2,3,5,10,1,5,3,10,10,10,10,65]
    
    
    a_2_1_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Census Tract", "Split County Indicator", 
    "Population Classification", "Income Group Total", "Report Level", "Number of Small Farm Loans Originated with Loan Amount at Origination < $100,000", 
    "Total Loan Amount of Small Farm Loans Originated with Loan Amount at Origination < $100,000", 
    "Number of Small Farm Loans Originated with Loan Amount at Origination > 100,000 and < or = to $250,000",
    "Total Loan Amount of Small Farm Loans Originated with Loan Amount at Origination > $100,000 and < or = to $250,000", 
    "Number of Small Farm Loans Originated with Loan Amount at  Origination > $250,000 and < or = to $500,000", 
    "Total Loan Amount of Small Farm Loans Originated with Loan Amount at Origination > $250,000 and < or = to $500,000",
    "Number of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million", 
    "Total Loan Amount of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    
    a_2_1_widths = [5,4,1,1,2,3,5,7,1,1,3,3,10,10,10,10,10,10,10,10,29]
    
    
    a_2_1a_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Respondent ID", "Agency Code", 
    "Number of Lenders", "Report Level", "Number of Small Farm Loans", "Total Loan Amount of Small Farm Loans",
    "Number of loans to Small Farms with Gross Annual Revenues < or = to $1 million",
    "Total Loan Amount of loans to Small Farms with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    a_2_1a_widths = [5,4,1,1,2,3,5,10,1,5,3,10,10,10,10,65]
    
    a_2_2_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Census Tract", "Split County Indicator",
    "Population Classification", "Income Group Total", "Report Level",
    "Number of Small Farm Loans Purchased with Loan Amount at Origination < or = to $100,000",
    "Total Loan Amount of Small Farm Loans Purchased with Loan Amount at Origination < or = to $100,000", 
    "Number of Small Farm Loans Purchased with Loan Amount at Origination > 100,000 and < or = to $250,000", 
    "Total Loan Amount of Small Farm Loans Purchased with Loan Amount at Origination > $100,000 and < or = to $250,000", 
    "Number of Small Farm Loans Purchased with Loan Amount at Origination > $250,000 and < or = to $500,000", 
    "Total Loan Amount of Small Farm Loans Purchased with Loan Amount at Origination > $250,000 and < or = to $500,000", 
    "Number of Small Farm Loans Purchased with Gross Annual Revenues < or = to $1 million",
    "Total Loan Amount of Small Farm Loans Purchased with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    a_2_2_widths = [5,4,1,1,2,3,5,7,1,1,3,3,10,10,10,10,10,10,10,10,29]
    
    a_2_2a_fields = ["Table ID","Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", "Respondent ID", "Agency Code", 
    "Number of Lenders", "Report Level", "Number of Small Farm Loans", "Total Loan Amount of Small Farm Loans",
    "Number of loans to Small Farms with Gross Annual Revenues < or = to $1 million", 
    "Total Loan Amount of loans to Small Farms with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    
    a_2_2a_widths = [5,4,1,1,2,3,5,10,1,5,3,10,10,10,10,65]
    
    
    d_1_1_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", 
                    "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Population Classification", 
                    "Income Group Total", "Report Level",
                    "Number of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000", 
                    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000", 
                    "Number of Small Business Loans Originated with Loan Amount at Origination > 100,000 and < or = to $250,000",
                    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $100,000 and < or = to $250,000", 
                    "Number of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000", 
                    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000",
                    "Number of Loans Originated to Small Businesses with Gross Annual Revenues < $1 million", 
                    "Total Loan Amount of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million", 
                    "Number of Small Business Loans Originated Reported as Affiliate Loans", 
                    "Total Loan Amount of Small Business Loans Originated Reported as Affiliate Loans"]
    
    d_1_1_widths = [5,10,1,4,1,1,2,3,5,4,1,1,1,3,3,10,10,10,10,10,10,10,10,10,10]
    
    
    d_1_2_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", 
                    "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Population Classification", 
                    "Income Group Total", "Report Level",
                    "Number of Small Business Loans Purchased with Loan Amount at Origination < or = to $100,000",
                    "Total Loan Amount of Small Business Loans Purchased with Loan Amount at Origination < or = to $100,000", 
                    "Number of Small Business Loans Purchased with Loan Amount at Origination > 100,000 and < or = to $250,000",
                    "Total Loan Amount of Small Business Loans Purchased with Loan Amount at Origination > $100,000 and < or = to $250,000",
                    "Number of Small Business Loans Purchased with Loan Amount at Origination > $250,000 and <or = to $1,000,000", 
                    "Total Loan Amount of Small Business Loans Purchased with Loan Amount at Origination > $250,000 and < $1,000,000", 
                    "Number of Small Business Loans Purchased with Gross Annual Revenues < or = to $1 million", 
                    "Total Loan Amount Small Business Loans Purchased with Gross Annual Revenues < or = to $1 million", 
                    "Number of Small Business Loans Purchased Reported as Affiliate Loans",
                    "Total Loan Amount of Small Business Loans Purchased Reported as Affiliate Loans"]
    
    d_1_2_widths = [5,10,1,4,1,1,2,3,5,4,1,1,1,3,3,10,10,10,10,10,10,10,10,10,10]
    
    
    
    d_2_1_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", 
                    "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Population Classification", 
                    "Income Group Total", "Report Level", 
                    "Number of Small Farm Loans Originated with Loan Amount at Origination < or = to $100,000", 
                    "Total Loan Amount of Small Farm Loans Originated with Loan Amount at Origination < or = to $100,000",
                    "Number of Small Farm Loans Originated with Loan Amount at Origination > 100,000 and < or = to $250,000",
                    "Total Loan Amount of Small Farm Loans Originated with Loan Amount at Origination > $100,000 and < $250,000", 
                    "Number of Small Farm Loans Originated with Loan Amount at Origination > $250,000 and < or = to $500,000", 
                    "Total Loan Amount of Small Farm Loans Originated with Loan Amount at Origination > $250,000 and < or = to $500,000",
                    "Number of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million", 
                    "Total Loan Amount of Loans Originated to Small Farms with Gross Annual Revenues < $1 million",
                    "Number of Small Farm Loans Originated Reported as Affiliate Loans",
                    "Total Loan Amount of Small Farm Originated Loans Reported as Affiliate Loans"]
    
    d_2_1_widths = [5,10,1,4,1,1,2,3,5,4,1,1,1,3,3,10,10,10,10,10,10,10,10,10,10]
    
    
    
    d_2_2_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "Action Taken Type", "State", "County", "MSA/MD", 
                    "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Population Classification", 
                    "Income Group Total", "Report Level",
                    "Number of Small Farm Loans Purchased with Loan Amount at Origination < or = to $100,000", 
                    "Total Loan Amount of Small Farm Loans Purchased with Loan Amount at Origination < or = to $100,000", 
                    "Number of Small Farm Loans Purchased with Loan Amount at Origination > 100,000 and < or = to $250,000",
                    "Total Loan Amount of Small Farm Loans Purchased with Loan Amount at Origination > $100,000 and < or = to $250,000", 
                    "Number of Small Farm Loans Purchased with Loan Amount at Origination > $250,000 and < $500,000", 
                    "Total Loan Amount of Small Farm Loans Purchased with Loan Amount at Origination > $250,000 and < $500,000", 
                    "Number of Small Farm Loans Purchased with Gross Annual Revenues < or = to $1 million",
                    "Total Loan Amount of Small Farm Loans Purchased with Gross Annual Revenues < $1 million", 
                    "Number of Small Farm Loans Purchased Reported as Affiliate Loans",
                    "Total Loan Amount of Small Farm Loans Purchased Reported as Affiliate Loans"]
    
    d_2_2_widths = [5,10,1,4,1,1,2,3,5,4,1,1,1,3,3,10,10,10,10,10,10,10,10,10,10]
    
    
    
    d3_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "State", "County", "MSA/MD", 
          "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Report Level", 
          "Number of Small Business Loans Originated", "Total Loan Amount of Small Business Loans Originated", 
          "Number of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million",
          "Total Loan Amount of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million", 
          "Number of Small Business Loans Purchased", "Total Loan Amount of Small Business Loans Purchased", "Filler"]
    
    d3_widths = [5,10,1,4,1,2,3,5,4,1,1,2,10,10,10,10,10,10,46]
    
    
    
    d4_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "State", "County", "MSA/MD", 
          "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Report Level",
          "Number of Small Farm Loans Originated", "Total Loan Amount of Small Farm Loans Originated",
          "Number of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million", 
          "Total Loan Amount of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million",
          "Number of Small Farm Loans Purchased", "Total Loan Amount of Small Farm Loans Purchased", "Filler"]
    
    
    d4_widths = [5,10,1,4,1,2,3,5,4,1,1,2,10,10,10,10,10,10,46]
    
    
    d5_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "Number of Loans",
                "Total Loan Amount of Loans", "Number of Loans Reported as Affiliate Loans",
                 "Total Loan Amount of Loans Reported as Affiliate Loans", "Action Type", "Filler"]
    
    
    d5_widths = [5,10,1,4,1,10,10,10,10,1,83]
    
    
    d6_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year","State", "County", "MSA/MD", "Census Tract",
                 "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Population Classification", 
                 "Income Group", "Loan Indicator", "Filler"]
    
    d6_widths = [5,10,1,4,2,3,5,7,4,1,1,1,3,1,96]

    # most_recent_year = max([re.findall(r'\d{4}',file)[0] for file in os.listdir('data') if 'cra' in file])
    # lst_of_files = file_lst
    
    fwf_dimensions_dict = {
        [file for file in file_lst if 'transmittal' in file.lower()][0]:[transmittal_widths,transmittal_fields],
        [file for file in file_lst if 'a11' in file.lower() and 'a11a' not in file.lower()][0]:[a_1_1_widths,a_1_1_fields],
        [file for file in file_lst if 'a11a' in file.lower()][0]:[a_1_1a_widths,a_1_1a_fields],
        [file for file in file_lst if 'a12' in file.lower() and 'a12a' not in file.lower()][0]:[a_1_2_widths,a_1_2_fields],
        [file for file in file_lst if 'a12a' in file.lower()][0]:[a_1_2a_widths,a_1_2a_fields],
        [file for file in file_lst if 'a21' in file.lower() and 'a21a' not in file.lower()][0]:[a_2_1_widths,a_2_1_fields],
        [file for file in file_lst if 'a21a' in file.lower()][0]:[a_2_1a_widths,a_2_1a_fields],
        [file for file in file_lst if 'a22' in file.lower() and 'a22a' not in file.lower()][0]:[a_2_2_widths,a_2_2_fields],
        [file for file in file_lst if 'a22a' in file.lower()][0]:[a_2_2a_widths,a_2_2a_fields],
        [file for file in file_lst if 'd11' in file.lower()][0]:[d_1_1_widths,d_1_1_fields],
        [file for file in file_lst if 'd12' in file.lower()][0]:[d_1_2_widths,d_1_2_fields],
        [file for file in file_lst if 'd21' in file.lower()][0]:[d_2_1_widths,d_2_1_fields],
        [file for file in file_lst if 'd22' in file.lower()][0]:[d_2_2_widths,d_2_2_fields],
        [file for file in file_lst if 'd3' in file.lower()][0]:[d3_widths,d3_fields],
        [file for file in file_lst if 'd4' in file.lower()][0]:[d4_widths,d4_fields],
        [file for file in file_lst if 'd5' in file.lower()][0]:[d5_widths,d5_fields],
        [file for file in file_lst if 'd6' in file.lower()][0]:[d6_widths,d6_fields]}
    
    df_dict = {}
    for i in os.listdir(data_folder):
        if i in fwf_dimensions_dict: 
            df_dict[i] = pd.read_fwf(os.path.join(data_folder, i), widths = fwf_dimensions_dict[i][0], header = None, names = fwf_dimensions_dict[i][1])
    return df_dict
    
def zero_adder(fips_code: str)->str:
    """Takes in a county code string and add zeroes to make it accuarte  to the fcc website county codes.
    
    Args:
        fips_code: a county fips code from the cra files
    
    Returns: 
        A county code string that matches the county portion of the county code on the fcc website.
    """
    if len(fips_code) == 1:
        return '00'+ fips_code
    elif len(fips_code) == 2:
        return '0' + fips_code
    else:
        return fips_code

def cra_mapping_function(df_dictionary: dict[str: pd.core.frame.DataFrame])->dict[str: pd.core.frame.DataFrame]:
    """Used to map full descriptions to data entires that use codes as place holders in cra data.
    
    Args:
        df_dictionary: a dictionary of dataframes reulting from the cra_data_ingester function.
    
    Returns: 
        A dictionary of cra data dataframes where the .dat cra data file name is the key and the corresponding dataframe is the value.
    """
    # save all file strings to variables 
    transmittal_sheet = [file for file in df_dictionary.keys() if 'transmittal' in file.lower()][0]
    a11 = [file for file in df_dictionary.keys() if 'a11' in file.lower() and 'a11a' not in file.lower()][0]
    a11a = [file for file in df_dictionary.keys() if 'a11a' in file.lower()][0]
    a12 = [file for file in df_dictionary.keys() if 'a12' in file.lower() and 'a12a' not in file.lower()][0]
    a12a = [file for file in df_dictionary.keys() if 'a12a' in file.lower()][0]
    a21 = [file for file in df_dictionary.keys() if 'a21' in file.lower() and 'a21a' not in file.lower()][0]
    a21a = [file for file in df_dictionary.keys() if 'a21a' in file.lower()][0]
    a22 = [file for file in df_dictionary.keys() if 'a22' in file.lower() and 'a22a' not in file.lower()][0]
    a22a = [file for file in df_dictionary.keys() if 'a22a' in file.lower()][0]
    d11 = [file for file in df_dictionary.keys() if 'd11' in file.lower()][0]
    d12 = [file for file in df_dictionary.keys() if 'd12' in file.lower()][0]
    d21 = [file for file in df_dictionary.keys() if 'd21' in file.lower()][0]
    d22 = [file for file in df_dictionary.keys() if 'd22' in file.lower()][0]
    d3 = [file for file in df_dictionary.keys() if 'd3' in file.lower()][0]
    d4 = [file for file in df_dictionary.keys() if 'd4' in file.lower()][0]
    d5 = [file for file in df_dictionary.keys() if 'd5' in file.lower()][0]
    d6 = [file for file in df_dictionary.keys() if 'd6' in file.lower()][0]
    # print(a11)
    # print(a11a)
    # print(a12)
    # print(a12a)
    # print(a21)
    # print(a21a)
    # print(a22)
    # print(a22a)
    # print(d11)
    # print(d12)
    # print(d21)
    # print(d22)
    # print(d3)
    # print(d4)
    # print(d5)
    # print(d6)
    
    # Transmittal Sheet 
    df_dictionary[transmittal_sheet]['Agency Code'] =  df_dictionary[transmittal_sheet]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC"
        })

    # A11
    df_dictionary[a11]['Loan Type'] = df_dictionary[a11]['Loan Type'].map({
        4:"Small Business", 
        })
    
    df_dictionary[a11]['Action Taken Type'] = df_dictionary[a11]['Action Taken Type'].map({
        1:"Originations"
    })

    df_dictionary[a11]['MSA/MD'] = df_dictionary[a11]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a11]['Census Tract'] = df_dictionary[a11]['Census Tract'].replace(np.nan, "totals")
    
    df_dictionary[a11]['Split County Indicator'] = df_dictionary[a11]['Split County Indicator'].map({
        "Y":"YES",
        "N":"NO"
    }).replace(np.nan, "total")
    
    df_dictionary[a11]['Population Classification'] = df_dictionary[a11]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with > 500,000 in population"
    }).replace(np.nan, "total")
    
    df_dictionary[a11]['Income Group Total'] = df_dictionary[a11]['Income Group Total'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "total")

    df_dictionary[a11]['Report Level'] = df_dictionary[a11]['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")
    
    #A11a
    df_dictionary[a11a]['Loan Type'] = df_dictionary[a11a]['Loan Type'].map({
        4:"Small Business", 
    })
    
    df_dictionary[a11a]['Action Taken Type'] = df_dictionary[a11a]['Action Taken Type'].map({
        1:"Originations"
    })

    df_dictionary[a11a]['MSA/MD'] = df_dictionary[a11a]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a11a]['Respondent ID'] = df_dictionary[a11a]['Respondent ID'].replace(np.nan, "total")
    
    df_dictionary[a11a]['Agency Code'] = df_dictionary[a11a]['Agency Code'].map({
        1:"OCC",
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary[a11a]['Number of Lenders'] = df_dictionary[a11a]['Number of Lenders'].replace(np.nan, "not a total")

    df_dictionary[a11a]['Report Level'] = df_dictionary[a11a]['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")


    #A12
    df_dictionary[a12]['Loan Type'] = df_dictionary[a12]['Loan Type'].map({
        4:"Small Business"
    })

    df_dictionary[a12]['Action Taken Type'] = df_dictionary[a12]['Action Taken Type'].map({
        6:"Purchases"
    })

    df_dictionary[a12]['MSA/MD'] = df_dictionary[a12]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a12]['Census Tract'] = df_dictionary[a12]['Census Tract'].replace(np.nan, "total")

    df_dictionary[a12]['Split County Indicator'] = df_dictionary[a12]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[a12]['Population Classification'] = df_dictionary[a12]['Population Classification'].map({
       "S":"counties with < or = to 500,000 in population",
       "L":"counties with > 500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary[a12]['Income Group Total'] = df_dictionary[a12]['Income Group Total'].map({
        1:"<10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "totals")

    df_dictionary[a12]['Report Level'] = df_dictionary[a12]['Report Level'].map({
       100:"Income Group Total",
       200:"County Total",
       210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A12a 
    df_dictionary[a12a]['Loan Type'] = df_dictionary[a12a]['Loan Type'].map({
        4:"Small Business", 
    })
    
    df_dictionary[a12a]['Action Taken Type'] = df_dictionary[a12a]['Action Taken Type'].map({
        6:"Purchases"
    })

    df_dictionary[a12a]['MSA/MD'] = df_dictionary[a12a]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a12a]['Respondent ID'] = df_dictionary[a12a]['Respondent ID'].replace(np.nan, "total")
    
    df_dictionary[a12a]['Agency Code'] = df_dictionary[a12a]['Agency Code'].map({
        1:"OCC",
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary[a12a]['Number of Lenders'] = df_dictionary[a12a]['Number of Lenders'].replace(np.nan, "total")

    df_dictionary[a12a]['Report Level'] = df_dictionary[a12a]['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A21
    df_dictionary[a21]['Loan Type'] = df_dictionary[a21]['Loan Type'].map({
        5:"Small Farm"        
    })

    df_dictionary[a21]['Action Taken Type'] = df_dictionary[a21]['Action Taken Type'].map({
        1:"Originations"        
    })

    df_dictionary[a21]['MSA/MD'] = df_dictionary[a21]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a21]['Census Tract'] = df_dictionary[a21]['Census Tract'].replace(np.nan, "total")

    df_dictionary[a21]['Split County Indicator'] = df_dictionary[a21]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"       
    }).replace(np.nan, "total")

    df_dictionary[a21]['Population Classification'] = df_dictionary[a21]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with > 500,000 in population" 
    }).replace(np.nan, "total")

    df_dictionary[a21]['Income Group Total'] = df_dictionary[a21]['Income Group Total'].map({
        1:"<10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "total")

    df_dictionary[a21]['Report Level'] = df_dictionary[a21]['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A21a 
    df_dictionary[a21a]['Loan Type'] = df_dictionary[a21a]['Loan Type'].map({
        5:"Small Farm" 
    })

    df_dictionary[a21a]['Action Taken Type'] = df_dictionary[a21a]['Action Taken Type'].map({
        1:"Originations"        
    })

    df_dictionary[a21a]['MSA/MD'] = df_dictionary[a21a]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")
    
    df_dictionary[a21a]['Respondent ID'] = df_dictionary[a21a]['Respondent ID'].replace(np.nan, "total")

    df_dictionary[a21a]['Agency Code'] = df_dictionary[a21a]['Agency Code'].map({
        1:"OCC",
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary[a21a]['Number of Lenders'] = df_dictionary[a21a]['Number of Lenders'].replace(np.nan, "total")

    df_dictionary[a21a]['Report Level'] = df_dictionary[a21a]['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A22
    df_dictionary[a22]['Loan Type'] = df_dictionary[a22]['Loan Type'].map({
         5:"Small Farm"
     })

    df_dictionary[a22]['Action Taken Type'] = df_dictionary[a22]['Action Taken Type'].map({
        6:"Purchases"
    })

    df_dictionary[a22]['MSA/MD'] = df_dictionary[a22]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a22]['Census Tract'] = df_dictionary[a22]['Census Tract'].replace(np.nan, "total")

    df_dictionary[a22]['Split County Indicator'] = df_dictionary[a22]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[a22]['Population Classification'] = df_dictionary[a22]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with > 500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary[a22]['Income Group Total'] = df_dictionary[a22]['Income Group Total'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)",          
    }).replace(np.nan, "total")

    df_dictionary[a22]['Report Level'] = df_dictionary[a22]['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A22a
    df_dictionary[a22a]['Loan Type'] = df_dictionary[a22a]['Loan Type'].map({
        5:"Small Farm"
    })

    df_dictionary[a22a]['Action Taken Type'] = df_dictionary[a22a]['Action Taken Type'].map({
        6:"Purchases"
    })

    df_dictionary[a22a]['MSA/MD'] = df_dictionary[a22a]['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary[a22a]['Respondent ID'] = df_dictionary[a22a]['Respondent ID'].replace(np.nan, "total")

    df_dictionary[a22a]['Agency Code'] = df_dictionary[a22a]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary[a22a]['Number of Lenders'] = df_dictionary[a22a]['Number of Lenders'].replace(np.nan, "total")

    df_dictionary[a22a]['Report Level'] = df_dictionary[a22a]['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # D11
    df_dictionary[d11]['Agency Code'] =  df_dictionary[d11]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d11]['Loan Type'] =  df_dictionary[d11]['Loan Type'].map({
        4:"Small Business"
    })

    df_dictionary[d11]['Action Taken Type'] =  df_dictionary[d11]['Action Taken Type'].map({
        1:"Originations"
    })

    df_dictionary[d11]['MSA/MD'] =  df_dictionary[d11]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d11]['Assessment Area Number'] =  df_dictionary[d11]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary[d11]['Partial County Indicator'] =  df_dictionary[d11]['Partial County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d11]['Split County Indicator'] =  df_dictionary[d11]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d11]['Population Classification'] =  df_dictionary[d11]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary[d11]['Income Group Total'] =  df_dictionary[d11]['Income Group Total'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "total")

    df_dictionary[d11]['Report Level'] =  df_dictionary[d11]['Report Level'].map({
        4:"Total Inside & Outside Assessment Area (AA) (across all states)",
        6:"Total Inside AA (across all states)",
        8:"Total Outside AA (across all states)",
        10:"State Total",
        20:"Total Inside AA in State",
        30:"Total Outside AA in State",
        40:"County Total",
        50:"Total Inside AA in County",
        60:"Total Outside AA in County"
    }).replace(np.nan, "not a total")

    # D12
    df_dictionary[d12]['Agency Code'] =  df_dictionary[d12]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d12]['Loan Type'] =  df_dictionary[d12]['Loan Type'].map({
        4:"Small Business"
    })

    df_dictionary[d12]['Action Taken Type'] =  df_dictionary[d12]['Action Taken Type'].map({
        6:"Purchases"
    })

    df_dictionary[d12]['MSA/MD'] =  df_dictionary[d12]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d12]['Assessment Area Number'] =  df_dictionary[d12]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary[d12]['Partial County Indicator'] =  df_dictionary[d12]['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d12]['Split County Indicator'] =  df_dictionary[d12]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d12]['Population Classification'] =  df_dictionary[d12]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary[d12]['Income Group Total'] =  df_dictionary[d12]['Income Group Total'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "total")

    df_dictionary[d12]['Report Level'] =  df_dictionary[d12]['Report Level'].map({
        4:"Total Inside & Outside Assessment Area (AA) (across all states)",
        6:"Total Inside AA (across all states)",
        8:"Total Outside AA (across all states)",
        10:"State Total",
        20:"Total Inside AA in State",
        30:"Total Outside AA in State",
        40:"County Total",
        50:"Total Inside AA in County",
        60:"Total Outside AA in County"
    }).replace(np.nan, "not a total")

    # D21
    df_dictionary[d21]['Agency Code'] =  df_dictionary[d21]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d21]['Loan Type'] =  df_dictionary[d21]['Loan Type'].map({
       5:"Small Farm"
    })

    df_dictionary[d21]['Action Taken Type'] =  df_dictionary[d21]['Action Taken Type'].map({
       1:"Originations"
    })

    df_dictionary[d21]['MSA/MD'] =  df_dictionary[d21]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d21]['Assessment Area Number'] =  df_dictionary[d21]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary[d21]['Partial County Indicator'] =  df_dictionary[d21]['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d21]['Split County Indicator'] =  df_dictionary[d21]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d21]['Population Classification'] =  df_dictionary[d21]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary[d21]['Income Group Total'] =  df_dictionary[d21]['Income Group Total'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "total")

    df_dictionary[d21]['Report Level'] =  df_dictionary[d21]['Report Level'].map({
        4:"Total Inside & Outside Assessment Area (AA) (across all states)",
        6:"Total Inside AA (across all states)",
        8:"Total Outside AA (across all states)",
        10:"State Total",
        20:"Total Inside AA in State",
        30:"Total Outside AA in State",
        40:"County Total",
        50:"Total Inside AA in County",
        60:"Total Outside AA in County"
    }).replace(np.nan, "not a total")

    # D22
    df_dictionary[d22]['Agency Code'] =  df_dictionary[d22]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d22]['Loan Type'] =  df_dictionary[d22]['Loan Type'].map({
       5:"Small Farm"
    })

    df_dictionary[d22]['Action Taken Type'] =  df_dictionary[d22]['Action Taken Type'].map({
       6:"Purchases"
    })

    df_dictionary[d22]['MSA/MD'] =  df_dictionary[d22]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d22]['Assessment Area Number'] =  df_dictionary[d22]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary[d22]['Partial County Indicator'] =  df_dictionary[d22]['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d22]['Split County Indicator'] =  df_dictionary[d22]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d22]['Population Classification'] =  df_dictionary[d22]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary[d22]['Income Group Total'] =  df_dictionary[d22]['Income Group Total'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> or = to 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> or = to 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    }).replace(np.nan, "total")

    df_dictionary[d22]['Report Level'] =  df_dictionary[d22]['Report Level'].map({
        4:"Total Inside & Outside Assessment Area (AA) (across all states)",
        6:"Total Inside AA (across all states)",
        8:"Total Outside AA (across all states)",
        10:"State Total",
        20:"Total Inside AA in State",
        30:"Total Outside AA in State",
        40:"County Total",
        50:"Total Inside AA in County",
        60:"Total Outside AA in County"
    }).replace(np.nan, "not a total")

    #D3
    df_dictionary[d3]['Agency Code'] =  df_dictionary[d3]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d3]['Loan Type'] =  df_dictionary[d3]['Loan Type'].map({
       4:"Small Business"
    })

    df_dictionary[d3]['MSA/MD'] = df_dictionary[d3]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d3]['Assessment Area Number'] =  df_dictionary[d3]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary[d3]['Partial County Indicator'] =  df_dictionary[d3]['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d3]['Split County Indicator'] =  df_dictionary[d3]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d3]['Report Level'] =  df_dictionary[d3]['Report Level'].map({
        5:"Assessment Area Total",
        10:"County Total within Assessment Area",
        15:"Activity Inside all Assessment Areas",
        20:"Activity Outside Assessment Area(s)",
        30:"Total Loans (Inside +Outside Assessment Area)"
    })

    #D4
    df_dictionary[d4]['Agency Code'] =  df_dictionary[d4]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d4]['Loan Type'] =  df_dictionary[d4]['Loan Type'].map({
      5:"Small Farm"
    })

    df_dictionary[d4]['MSA/MD'] = df_dictionary[d4]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d4]['Assessment Area Number'] =  df_dictionary[d4]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary[d4]['Partial County Indicator'] =  df_dictionary[d4]['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d4]['Split County Indicator'] =  df_dictionary[d4]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary[d4]['Report Level'] =  df_dictionary[d4]['Report Level'].map({
        5:"Assessment Area Total",
        10:"County Total within Assessment Area",
        15:"Activity Inside all Assessment Areas",
        20:"Activity Outside Assessment Area(s)",
        30:"Total Loans (Inside +Outside Assessment Area)"
    })

    #D5
    df_dictionary[d5]['Agency Code'] =  df_dictionary[d5]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary[d5]['Loan Type'] =  df_dictionary[d5]['Loan Type'].map({
        6:"Community Development",
        7:"Consortium/Third-Party"
    })

    df_dictionary[d5]['Action Type'] =  df_dictionary[d5]['Action Type'].map({
        "O":"Originated",
        "P":"Purchased",
        "T":"Total (Originated + Purchased)"
    })
    
    #D6
    df_dictionary[d6]['Agency Code'] =  df_dictionary[d6]['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary[d6]['MSA/MD'] = df_dictionary[d6]['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary[d6]['Census Tract'] = df_dictionary[d6]['Census Tract'].apply(format_census_tract)

    df_dictionary[d6]['Assessment Area Number'] =  df_dictionary[d6]['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area(s) (including predominately military areas)")

    df_dictionary[d6]['Partial County Indicator'] =  df_dictionary[d6]['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    })

    df_dictionary[d6]['Split County Indicator'] =  df_dictionary[d6]['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    })

    df_dictionary[d6]['Population Classification'] =  df_dictionary[d6]['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with >500,000 in population"
    })

    df_dictionary[d6]['Income Group'] =  df_dictionary[d6]['Income Group'].map({
        1:"< 10% of Median Family Income(MFI)",
        2:"10% to 20% of MFI",
        3:"20% to 30% of MFI",
        4:"30% to 40% of MFI",
        5:"40% to 50% of MFI",
        6:"50% to 60% of MFI",
        7:"60% to 70% of MFI",
        8:"70% to 80% of MFI",
        9:"80% to 90% of MFI",
        10:"90% to 100% of MFI",
        11:"100% to 110% of MFI",
        12:"110% to 120% of MFI",
        13:"> 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (Reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    })

    df_dictionary[d6]['Loan Indicator'] =  df_dictionary[d6]['Loan Indicator'].map({
        "Y":"Yes",
        "N":"No"
    })

    return df_dictionary

def state_county_fips_mapper(df_dict: dict[pd.core.frame.DataFrame],
                             fcc_fips_dict: dict[str: dict[str: str]]) -> dict[str: pd.core.frame.DataFrame]:
    """Used to map state and county names to their corresponding fips codes in cra data.
    
    Args:
        df_dict: a dictionary of dataframes reulting from the cra_mapping_function function.
        fcc_fips_dict: a dictioanry of state and county fips codes resulting from the fcc_fips_mappings_getter function.
    
    Returns: 
        A dictionary of cra data dataframes with state and county names mapped in. The .dat cra data file name is the key and the corresponding dataframe is the value.
    """
    for i in df_dict.keys():
        if 'State' and 'County' in df_dict[i].columns:
            print(i)
            df_dict[i]['County'] = df_dict[i]['County'].astype(str).apply(lambda x: x.replace('.0','')) 
            df_dict[i]['State'] = df_dict[i]['State'].astype(str).apply(lambda x: x.replace('.0', '')) 
            df_dict[i]['State'] = df_dict[i]['State'].apply(lambda x: '0'+ x if len(x)<2 else x) 
            df_dict[i]['County'] = df_dict[i]['County'].apply(zero_adder)
            df_dict[i]['County'] = df_dict[i]['State'] + df_dict[i]['County']
            df_dict[i]['State_Name'] = df_dict[i]['State'].map(fcc_fips_dict['fcc_states'])
            df_dict[i]['County_Name'] = df_dict[i]['County'].map(fcc_fips_dict['fcc_counties'])
            df_dict[i]['State'] = df_dict[i]['State_Name']
            df_dict[i]['County'] = df_dict[i]['County_Name']
            df_dict[i] = df_dict[i].drop(columns = ['State_Name','County_Name'], axis = 1)
    return df_dict

def thousands_adder(df_dict: dict[str: pd.core.frame.DataFrame]) -> dict[str: pd.core.frame.DataFrame]:
    """" Multiplies all fields in cra data that contain total loan amounts by 1000, subsets all files in dictionary for disclosure 1-1 and disclosure 6 datasets, then subsets those datasets for entires in Texas and in counties of focus.
        
    Args:
        df_dict: A dictionary of cra dataframes reulting from the state_county_fips_mapper function. 
        
    Returns:
         A dictionary of cra dataframes with loan amount columns showing their full amount(e.g. 153 is not 153000)
         """
    # multiply all fields containing "total loan amount" by 1000
    for file_name in df_dict.keys():
        for column in df_dict[file_name].columns:
            if 'Total Loan Amount'in column:
                #print(file_name, column)
                df_dict[file_name][column] = df_dict[file_name][column]*1000 

    transmittal_sheet = [file for file in df_dict.keys() if 'transmittal' in file.lower()][0]
    a11 = [file for file in df_dict.keys() if 'a11' in file.lower() and 'a11a' not in file.lower()][0]
    a11a = [file for file in df_dict.keys() if 'a11a' in file.lower()][0]
    a12 = [file for file in df_dict.keys() if 'a12' in file.lower() and 'a12a' not in file.lower()][0]
    a12a = [file for file in df_dict.keys() if 'a12a' in file.lower()][0]
    a21 = [file for file in df_dict.keys() if 'a21' in file.lower() and 'a21a' not in file.lower()][0]
    a21a = [file for file in df_dict.keys() if 'a21a' in file.lower()][0]
    a22 = [file for file in df_dict.keys() if 'a22' in file.lower() and 'a22a' not in file.lower()][0]
    a22a = [file for file in df_dict.keys() if 'a22a' in file.lower()][0]
    d11 = [file for file in df_dict.keys() if 'd11' in file.lower()][0]
    d12 = [file for file in df_dict.keys() if 'd12' in file.lower()][0]
    d21 = [file for file in df_dict.keys() if 'd21' in file.lower()][0]
    d22 = [file for file in df_dict.keys() if 'd22' in file.lower()][0]
    d3 = [file for file in df_dict.keys() if 'd3' in file.lower()][0]
    d4 = [file for file in df_dict.keys() if 'd4' in file.lower()][0]
    d5 = [file for file in df_dict.keys() if 'd5' in file.lower()][0]
    d6 = [file for file in df_dict.keys() if 'd6' in file.lower()][0]

    # fill in leading zeroes for right justified Respondent IDs
    df_dict[transmittal_sheet]['Respondent ID'] = df_dict[transmittal_sheet]['Respondent ID'].apply(fmt_respondent_id)
    df_dict[d11]['Respondent ID'] = df_dict[d11]['Respondent ID'].apply(fmt_respondent_id)
    df_dict[d6]['Respondent ID'] = df_dict[d6]['Respondent ID'].apply(fmt_respondent_id)

    # rename 'respondent' fields to 'institution' in transmittal sheet
    df_dict[transmittal_sheet] = df_dict[transmittal_sheet].rename(columns = {
        'Respondent Name':'Institution name',
        'Respondent Address':'Institution address',
        'Respondent City':'Institution city',
        'Respondent State':'Institution state',
        'Respondent Zip Code':'Institution zip code',
        'Assets':'Institution assets'})
    
    # filter Discl 11 down to Texas and Tarrant, Collin, and Dallas counties
    df_dict[d11] = df_dict[d11][df_dict[d11]['State'] == 'TEXAS']
    df_dict[d11] = df_dict[d11][(df_dict[d11]['County'] == 'Tarrant County') | (df_dict[d11]['County'] == 'Collin County') | (df_dict[d11]['County'] == 'Dallas County')]
    
    # filter Discl 6 down to Texas and Tarrant, Collin, and Dallas counties
    df_dict[d6] = df_dict[d6][df_dict[d6]['State'] == 'TEXAS']
    df_dict[d6] = df_dict[d6][(df_dict[d6]['County'] == 'Tarrant County') | (df_dict[d6]['County'] == 'Collin County') | (df_dict[d6]['County'] == 'Dallas County')]

    # remove leading and trailing whitespace from column of all dataset3s 
    df_dict[a11] = df_dict[a11].rename(columns = lambda x: x.strip())
    df_dict[a11a] = df_dict[a11a].rename(columns = lambda x: x.strip())
    df_dict[a12] = df_dict[a12].rename(columns = lambda x: x.strip())
    df_dict[a12a] = df_dict[a12a].rename(columns = lambda x: x.strip())
    df_dict[a21] = df_dict[a21].rename(columns = lambda x: x.strip())
    df_dict[a21a] = df_dict[a21a].rename(columns = lambda x: x.strip())
    df_dict[a22] = df_dict[a22].rename(columns = lambda x: x.strip())
    df_dict[a22a] = df_dict[a22a].rename(columns = lambda x: x.strip()) 
    df_dict[d11] = df_dict[d11].rename(columns = lambda x: x.strip())
    df_dict[d12] = df_dict[d12].rename(columns = lambda x: x.strip())
    df_dict[d21] = df_dict[d21].rename(columns = lambda x: x.strip())
    df_dict[d22] = df_dict[d22].rename(columns = lambda x: x.strip())
    df_dict[d3] = df_dict[d3].rename(columns = lambda x: x.strip())
    df_dict[d4] = df_dict[d4].rename(columns = lambda x: x.strip())
    df_dict[d5] = df_dict[d5].rename(columns = lambda x: x.strip())
    df_dict[d6] = df_dict[d6].rename(columns = lambda x: x.strip())

    # Only keep D11 and D6 (to cut down on memory issues)
    final_cra_dict = {d11: df_dict[d11], # Small business loans by County level
                      d6: df_dict[d6],  # Assessment area by census tract
                      transmittal_sheet: df_dict[transmittal_sheet]
                        }
    return final_cra_dict

# fdic helper function
def changec_label_adder(data_folder: str,file_name: str)->dict[str: str]:
    """Used to create dictionary of old column names as the key and new column names as the value using the institutions definitions file.
    
    Args: 
        file_name: Name of institutions definitions data file
        
    Returns:
        a dictionary of old column names as the keys and new column names as the values
        """
    institutions_definitions_df = pd.read_csv(os.path.join(data_folder,file_name))
    col_name_replace_map = dict(zip(institutions_definitions_df['Variable Name'],institutions_definitions_df['Variable Label']))  
    # loop through the dictionary create between the variable name and variable label fields to add numbers to distinguish the CHANGEC
    # values
    for original_field in col_name_replace_map.keys():
        if "CHANGEC" in original_field: 
            col_name_replace_map[original_field] = col_name_replace_map[original_field] + " " + original_field.split('CHANGEC')[1]
    col_name_replace_map['FED_RSSD'] = 'Federal Reserve ID Number 2'
    col_name_replace_map['TRACT'] = 'Institutions with reportable fiduciary related service'
    return col_name_replace_map  

def fdic_institutions_ingester(data_folder: str,
                               institutions_file_name: str, 
                               col_replace_map: dict[str: str], 
                               analysis_yr: str) -> pd.core.frame.DataFrame:
    """Used to read in institution data for those created on or before 12/31/analysis_year and are in dallas, collins or tarrant county.
    
    Args: 
        institutions_file_name: Name of institutions data file
        col_replace_map: dictionary containing columns names returned from changec_label_adder() function.
        
    Returns:
        A dataframe of the fdic institutions data
        """
    year_upper_bound = str(analysis_yr) + '-12-31'
    institutions_df = pd.read_csv(os.path.join(data_folder,institutions_file_name))
    institutions_df = institutions_df.rename(columns = col_replace_map)
    # map in values for columns using numbers to represnet description
    institutions_df['Institution Status'] = institutions_df['Institution Status'].map({ 1:'Institutions that are currently open and insured by the FDIC',
                                                                                        0:'Institution closed or not insured by FDIC'})  
    institutions_df["Institution Class"] = institutions_df["Institution Class"].map({"NM":"Commercial bank, state charter, Fed non-member, and supervised by the Federal Deposit Insurance Corporation (FDIC)",
      "SI":"State chartered stock savings banks, supervised by the FDIC",
      "N":"Commercial bank, national (federal) charter, Fed member, and supervised by the Office of the Comptroller of the Currency (OCC)", 
      "NC":"Noninsured non-deposit commercial banks and/or trust companies regulated by the OCC, a state, or a territory",
      "SM":"Commercial bank, state charter, Fed member, and supervised by the Federal Reserve Bank (FRB)",
      "SB":"Federal savings banks, federal charter, supervised by the OCC or before July 21,2011 the Office of Thrift Supervision (OTS)",
      "SL":"State chartered stock savings and loan associations, supervised by the FDIC or before July 21,2011 the OTS",
      "NS":"Noninsured stock savings bank supervised by a state or territory",
      "OI":"Insured U.S. branch of a foreign chartered institution (IBA) and supervised by the OCC or FDIC",
      "CU":"state or federally chartered credit unions supervised by the National Credit Union Association (NCUA)"})
    institutions_df['Metropolitan Divisions Flag'] = institutions_df['Metropolitan Divisions Flag'].map({1:"Yes",0:"No"})
    institutions_df['Metropolitan Division Flag'] = institutions_df['Metropolitan Division Flag'].map({1:"Yes",0:"No"})
    institutions_df['Micropolitan Division Flag'] = institutions_df['Micropolitan Division Flag'].map({1:"Yes",0:"No"})
    institutions_df['CFPB Flag'] = institutions_df['CFPB Flag'].map({'0':"not supervised by CFPB",'1':"secondarily supervised by CFPB"})
    institutions_df['CSA Area Flag'] = institutions_df['CSA Area Flag'].map({1:"Yes",0:"No"})
    institutions_df['Numeric Code'] = institutions_df['Numeric Code'].map({"03":"National bank, Federal Reserve System (FRS) member",
          "13":"State commercial bank, FRS member",
          "15":"State industrial bank, FRS member",
          "21":"State commercial bank, not FRS member",
          "23":"State industrial bank, not FRS member",
          "25":"State nonmember mutual bank",
          "33":"Federal chartered savings and co-operative bank - stock",
          "34":"Federal chartered savings and co-operative bank - mutual",
          "35":"State chartered thrift - stock",
          "36":"State chartered thrift - mutual",
          "37":"Federal chartered thrift - stock",
          "38":"Federal chartered thrift - mutual",
          "41":"State chartered stock savings and co-operative bank",
          "42":"State chartered mutual savings and co-operative bank",
          "43":"Federal chartered stock savings bank (historical)",
          "44":"Federal chartered mutual savings bank (historical)", 
          "50":"Nondeposit trust company, OCC chartered",
          "51":"Commercial bank",
          "53":"Industrial bank",
          "54":"Nondeposit trust company, state chartered, not FRS member",
          "57":"New York investment company",
          "58":"Nondeposit trust company, state chartered, FRS member",
          "59":"Nondeposit trust company",
          "61":"Noninsured private bank",
          "62":"Noninsured loan workout bank, OCC chartered",
          "63":"Noninsured loan workout bank, state chartered, FRS member",
          "64":"Noninsured loan workout bank, state chartered, not FRS member",
          "81":"Noninsured stock savings and co-operative bank",
          "82":"Noninsured mutual savings and co-operative bank",
          "85":"Noninsured stock savings and loan association",
          "86":"Noninsured mutual savings and loan association",
          "89":"Noninsured insurance company"})
    institutions_df['Conservatorship'] = institutions_df['Conservatorship'].map({1:"Yes",0:"No"})
    institutions_df['CSA Area Flag'] = institutions_df['CSA Area Flag'].map({1:"Yes",0:"No"})
    institutions_df = institutions_df.astype({'Federal Reserve ID Number': str})
    institutions_df['Federal Reserve ID Number'] = institutions_df['Federal Reserve ID Number'].map({"1":"Boston",
                                                                                                     "2":"New York",
                                                                                                     "3":"Philadelphia",
                                                                                                     "4":"Cleveland",
                                                                                                     "5":"Richmond",
                                                                                                     "6":"Atlanta",
                                                                                                     "7":"Chicago",
                                                                                                     "8":"St. Louis",
                                                                                                     "9":"Minneapolis",
                                                                                                     "10":"Kansas city",
                                                                                                     "11":"Dallas", 
                                                                                                     "12":"San Francisco"})
    institutions_df['Primary Regulator'] = institutions_df['Primary Regulator'].map({"OCC":"Office of the Comptroller of Currency",
                                                                                     "FDIC":"Federal Deposit Insurance Corporation",
                                                                                     "FRB":"Federal Reserve Board",
                                                                                     "NCUA":"National Credit Union Association",
                                                                                     "OTS":"Office of Thrift Supervision"})
    institutions_df['Supervisory Region Number'] = institutions_df['Supervisory Region Number'].map({2:"New York",
                                                                                                     5:"Atlanta",
                                                                                                     9:"Chicago",
                                                                                                     11:"Kansas City",
                                                                                                     13:"Dallas",
                                                                                                     14:"San Francisco",
                                                                                                     16:"Office of Complex Financial Institutions (CFI)"})
    institutions_df['Trust Powers'] = institutions_df['Trust Powers'].map({00:"Trust Powers Not Known",
                                                                           "10":"Full Trust Powers Granted",
                                                                           "11":"Full Trust Powers Granted, Exercised",
                                                                           "12":"Full Trust Powers Granted, Not Exercised",
                                                                           "20":"Limited Trust Powers Granted",
                                                                           "21":"Limited Trust Powers Granted, Exercised",
                                                                           "30":"Trust Powers Not Granted",
                                                                           "31":"Trust Powers Not Granted, But Exercised",
                                                                           "40":"Trust Powers Grandfathered"})
    institutions_df['State Charter'] = institutions_df['State Charter'].map({1:"yes",0:"no"})
    institutions_df['FFIEC Call Report 31 Filer'] = institutions_df['FFIEC Call Report 31 Filer'].map({1:"yes",0:"no"})
    institutions_df['Bank Holding Company Type'] = institutions_df['Bank Holding Company Type'].map({1:"yes", 0:"no"})
    institutions_df['Deposit Insurance Fund member'] = institutions_df['Deposit Insurance Fund member'].map({1:"Yes", 0:"No"})
    institutions_df['Law Sasser Flag'] = institutions_df['Law Sasser Flag'].map({1:"Yes", 0:"No"})
    institutions_df = institutions_df.astype({'Credit Card Institutions':int}, errors = 'ignore')
    # filter for established in 12/31/analysis_year or before
    institutions_df['Established Date'] = pd.to_datetime(institutions_df['Established Date'])
    institutions_df[institutions_df['Established Date'] <= year_upper_bound]
    # filter for dallas, collins and tarrant counties in TX
    institutions_df = institutions_df[institutions_df['State Alpha code'] == 'TX']
    institutions_df = institutions_df[(institutions_df['County'] == 'Tarrant') | (institutions_df['County'] == 'Collin') | (institutions_df['County'] == 'Dallas')]
    institutions_df = institutions_df.reset_index().set_index('FDIC Certificate #').reset_index()
    institutions_df = institutions_df.rename(columns = lambda x: x.strip())
    return institutions_df

def fdic_locations_mapper(data_folder: str,locations_def_file: str, locations_file: str, analysis_yr: str)->pd.core.frame.DataFrame:
    """Used to read in locations data for those created on or before 12/31/analysis_year and are in dallas, collins or tarrant county.
    
    Args: 
        locations_file: Name of locations data file
        locations_def_file: Name of locations definitions file
        
    Returns:
        A dataframe of of the fdic locations data
    """ 
    year_upper_bound = str(analysis_yr) + '-12-31'
    loc_fed_df = pd.read_csv(os.path.join(data_folder,locations_def_file))
    bkclass_replace_map = dict(zip(loc_fed_df.iloc[2:8,:]['TITLE'].str.replace(' ','').str.strip('-'), loc_fed_df.iloc[2:8,:]['DEFINITION']))
    serve_type_map = dict(zip(loc_fed_df.iloc[31:47,:]['TITLE'],loc_fed_df.iloc[31:47,:]['DEFINITION']))
    inst_col_name_map = dict(zip(loc_fed_df[loc_fed_df['NAME'].notnull()]['NAME'], loc_fed_df[loc_fed_df['NAME'].notnull()]['TITLE']))
    fdic_locations_df = pd.read_csv(os.path.join(data_folder,locations_file))
    fdic_locations_df['BKCLASS'] = fdic_locations_df['BKCLASS'].map(bkclass_replace_map)
    fdic_locations_df['SERVTYPE'] = fdic_locations_df['SERVTYPE'].map(serve_type_map)
    fdic_locations_df.rename(columns = inst_col_name_map, inplace = True)
    # map descriptions to columns using codes as place holders
    fdic_locations_df['Metropolitan Divisions Flag (Branch)'] = fdic_locations_df['Metropolitan Divisions Flag (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Metropolitan Division Flag (Branch)'] = fdic_locations_df['Metropolitan Division Flag (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Micropolitan Division Flag (Branch)'] = fdic_locations_df['Micropolitan Division Flag (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Combined Statistical Area Flag  (Branch)'] = fdic_locations_df['Combined Statistical Area Flag  (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Branch Established Date'] = pd.to_datetime(fdic_locations_df['Branch Established Date'])
    fdic_locations_df = fdic_locations_df[fdic_locations_df['Branch Established Date'] <= year_upper_bound]
    # filter for location in texas and counties that are dallas, collins or tarrant
    fdic_locations_df = fdic_locations_df[fdic_locations_df['Branch State   '] == 'Texas']
    final_fdic_locations_df = fdic_locations_df[(fdic_locations_df['Branch County'] == 'Tarrant') | (fdic_locations_df['Branch County'] == 'Collin') | (fdic_locations_df['Branch County'] == 'Dallas')]
    final_fdic_locations_df = final_fdic_locations_df.rename(columns = lambda x: x.strip())
    # create county column based on zipcodes column
    zips1 = county_to_countyzip_dict(["Dallas","Collin","Tarrant"])
    final_fdic_locations_df['County_from_Zipcode'] = final_fdic_locations_df['Branch Zip Code'].apply(str).apply(lambda x: zip_to_county_name(x,zips1))

    # create columns for census tract and county  based on branch address
    # final_fdic_locations_df['Full Branch Address'] = final_fdic_locations_df['Branch Address'] + ', ' + final_fdic_locations_df['Branch City'] + ', ' + final_fdic_locations_df['Branch State Abbreviation'] + ' ' + final_fdic_locations_df['Branch Zip Code'].apply(str)
    # tqdm.pandas()
    # final_fdic_locations_df['data_from_adr_geocode'] = final_fdic_locations_df['Full Branch Address'].progress_apply(lambda x: get_census_geocode(x))
    # final_fdic_locations_df['county_from_address'] = final_fdic_locations_df['data_from_adr_geocode'].apply(lambda x: x['county'])
    # final_fdic_locations_df['county_from_address'] = final_fdic_locations_df['county_from_address'] + ' County'
    # final_fdic_locations_df['census_tract_from_address'] = final_fdic_locations_df['data_from_adr_geocode'].apply(lambda x: x['census_tract'])
    # final_fdic_locations_df['census_tract_from_address'] = final_fdic_locations_df['census_tract_from_address'].fillna(value = np.nan).apply(float)/100
    # final_fdic_locations_df['census_tract_from_address'] = final_fdic_locations_df['census_tract_from_address'].apply(format_census_tract)
    # final_fdic_locations_df = final_fdic_locations_df.drop(columns = ['Full Branch Address'])

    # create a subset dataset of the fdic locations data that will be used to batch search for census and related geographic information
    final_fdic_locations_df = final_fdic_locations_df.reset_index()
    final_fdic_locations_df['index'] = final_fdic_locations_df['index'].apply(str)
    final_fdic_locations_df['idx_branch_number'] = '(' + final_fdic_locations_df['index'] +') '+  final_fdic_locations_df['Branch Number'].apply(str)
    final_fdic_locations_df[['idx_branch_number','Branch Address','Branch City', 'Branch State Abbreviation','Branch Zip Code']].set_index('idx_branch_number').to_csv(os.path.join(data_folder,'fdic_locations_sample.csv'))

    # lookup census codes for batch  
    start = time.time()
    b = census_batch_lookup(os.path.join(data_folder,'fdic_locations_sample.csv'), 'Branch')
    end = time.time()
    print('process completed in',end - start, 'seconds')

    # merge in new census codes to original dataset
    b = b.reset_index()
    b['index'] = b['Branch identifier'].str.split(')').replace('(','').apply(lambda x: x[0].replace('(',''))
    final_fdic_locations_df = pd.merge(final_fdic_locations_df,b, left_on = ['index'], right_on = ['index'], how = 'left')

    # reformat census tract column
    final_fdic_locations_df['_tract'] = final_fdic_locations_df['_tract'].apply(float)/100
    final_fdic_locations_df['_tract'] = final_fdic_locations_df['_tract'].apply(format_census_tract)

    # map in state and county names 
    url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
    fips_dict = fcc_fips_mappings_getter(url)
    final_fdic_locations_df['_county_fips_code'] = final_fdic_locations_df['_state_fips_code'].apply(str) + final_fdic_locations_df['_county_fips_code'].apply(str)
    final_fdic_locations_df['_county_fips_code'] = final_fdic_locations_df['_county_fips_code'].map(fips_dict['fcc_counties'])
    final_fdic_locations_df['_state_fips_code'] = final_fdic_locations_df['_state_fips_code'].map(fips_dict['fcc_states'])
    final_fdic_locations_df = final_fdic_locations_df.rename(columns = {'_county_fips_code':'_fips_county_name', '_state_fips_code':'_fips_state_name'})
    final_fdic_locations_df['Branch County'] = final_fdic_locations_df['Branch County'] + ' County'
    
    return final_fdic_locations_df

# sba helper function
def sba_data_ingester(url: str,
                      analysis_yr: str,
                      data_folder: str) -> pd.core.frame.DataFrame:  
    """Takes in url of sba FOIA - 7(a) csv file, downloads it, cleans it and retuns a pandas dataframe of the file.
    
    Args:
        url: The url of the downloaded csv file from the census tract website.
        
    Returns:
         A dataframe of the downloaded and cleaned csv file.    
    """
    file_name = url.split('/')[-1]
    # way 2
    # sba_file_year = '20' + re.findall('\d+',file.split('-')[-1])[0][0:2] # split file name, grab portion containing data, get year 
    year_lower_bound = str(int(analysis_yr) - 1) + '-12-31'
    year_upper_bound = str(int(year_lower_bound.split('-')[0]) + 2) + '-01-01'
    r = requests.get(url, allow_redirects = True)
    open(file_name,'wb').write(r.content)
    foia_7a_df = pd.read_csv(file_name, encoding = 'latin-1')
    # lower all column names and rename soldsecmr string containing column to SOLDSECMRTIND
    foia_7a_df.columns = ['SOLDSECMRTIND' if 'soldsecmr' in column_name else column_name.lower() for column_name in foia_7a_df.columns]
    # map in vales for intries in columns
    foia_7a_df['deliverymethod'] = foia_7a_df['deliverymethod'].map({
          "CA":"Community Advantage",
          "CLP":"Certified Lenders Program",
          "COMM EXPRS":"Community Express (inactive)",
          "DFP":"Dealer Floor Plan (inactive)",
          "DIRECT":"Direct Loan (inactive)",
          "EWCP":"Export Working Capital Program",
          "EXP CO GTY":"Co-guaranty with Export-Import Bank (inactive)",
          "EXPRES EXP":"Export Express",
          "GO LOANS":"Gulf Opportunity Loan (inactive)",
          "INTER TRDE":"International Trade",
          "OTH 7A":"Other 7(a) Loan",
          "PATRIOT EX":"Patriot Express (inactive)",
          "PLP":"Preferred Lender Program",
          "RLA":"Rural Lender Advantage (inactive)",
          "SBA EXPRES":"SBA Express",
          "SLA":"Small Loan Advantage",
          "USCAIP":"US Community Adjustment and Investment Program",
          "Y2K":"Y2K Loan (inactive)"
    })
    
    foia_7a_df['loanstatus'] = foia_7a_df['loanstatus'].map({
          "COMMIT":"Undisbursed",
          "PIF":"Paid In Full",
          "CHGOFF":"Charged Off",
          "CANCLD":"Cancelled",
          "EXEMPT":"The status of loans that have been disbursed but have not been cancelled, paid in full, or charged off are exempt from disclosure under FOIA Exemption 4"
    })
    
    foia_7a_df['revolverstatus'] =  foia_7a_df['revolverstatus'].map({
        0:"Term",
        1:"Revolver"
    })
    
    foia_7a_df['SOLDSECMRTIND'] = foia_7a_df['SOLDSECMRTIND'].map({
        "Y":"Sold on the secondary market",
        "N":"Not sold on the secondary market"
    })
    
    test_dct = {'borrname':"Borrower name",
    "borrstreet":"Borrower street address",
    "borrcity":"Borrower city",
    "borrstate":"Borrower state",
    "borrzip":"Borrower zip code",
    "grossapproval":"Total loan amount",
    "subpgmdesc":"Subprogram description"}
    
    new_columns = [test_dct.get(column) if column in test_dct.keys() else column for column in foia_7a_df.columns]
    foia_7a_df.columns = new_columns

    # map in full state names
    ssa_url = 'https://www.ssa.gov/international/coc-docs/states.html'
    state_abbrev_map = state_abrevs_getter(ssa_url)
    foia_7a_df['Borrower state'] = foia_7a_df['Borrower state'].map(state_abbrev_map)

    # subset for entries that have an approval date in analysis year and remove leading and trailing whitespace from columns names
    foia_7a_df['approvaldate'] = pd.to_datetime(foia_7a_df['approvaldate'], format = '%m/%d/%Y')
    foia_7a_df = foia_7a_df[(foia_7a_df['approvaldate'] > year_lower_bound) & (foia_7a_df['approvaldate'] < year_upper_bound)]
    foia_7a_df = foia_7a_df[foia_7a_df['Borrower state'] == 'TEXAS']
    foia_7a_df = foia_7a_df.rename(columns = lambda x: x.strip())

    # create county column based on zipcodes column 
    zips1 = county_to_countyzip_dict(["Dallas","Collin","Tarrant"])
    foia_7a_df['County_from_Zipcode'] = foia_7a_df['Borrower zip code'].apply(str).apply(lambda x: zip_to_county_name(x,zips1))

    # create columns for census tract and county  based on borrower address
    #foia_7adf['Full Address'] = foia_7a_df['Borrower street address'] + ', ' + foia_7a_df['Borrower city'] + ', ' + foia_7a_df['Borrower state'] + ' ' + foia_7a_df['Borrower zip code'].apply(str)
    # tqdm.pandas()
    # foia_7a_df['data_from_adr_geocode'] = foia_7a_df['Full Address'].progress_apply(lambda x: get_census_geocode(x))
    # foia_7a_df['county_from_address'] = foia_7a_df['data_from_adr_geocode'].apply(lambda x: x['county'])
    # foia_7a_df['county_from_address'] = foia_7a_df['county_from_address'] + ' County'
    # foia_7a_df['census_tract_from_address'] = foia_7a_df['data_from_adr_geocode'].apply(lambda x: x['census_tract'])
    # foia_7a_df['census_tract_from_address'] = foia_7a_df['census_tract_from_address'].fillna(value = np.nan).apply(float)/100
    # foia_7a_df['census_tract_from_address'] = foia_7a_df['census_tract_from_address'].apply(format_census_tract)
    # foia_7a_df = foia_7a_df.drop(columns = ['Full Address'])

    # create a subset dataset of the sba data that will be used to batch search for census and related geographic information 
    foia_7a_df = foia_7a_df.reset_index()
    foia_7a_df['index'] = foia_7a_df['index'].apply(str)
    foia_7a_df['Borrower name'] = '(' + foia_7a_df['index'] +') '+  foia_7a_df['Borrower name'] 
    foia_7a_df[['Borrower name','Borrower street address','Borrower city', 'Borrower state','Borrower zip code']].set_index('Borrower name').to_csv(os.path.join(data_folder,'sba_sample.csv'))

    # lookup census codes for batch  
    start = time.time()
    b = census_batch_lookup(os.path.join(data_folder,'sba_sample.csv'), 'borrower')
    end = time.time()
    print('process completed in',end - start, 'seconds')

    # merge in new census codes to original dataset
    b = b.reset_index()
    b['index'] = b['borrower identifier'].str.split(')').replace('(','').apply(lambda x: x[0].replace('(',''))
    foia_7a_df = pd.merge(foia_7a_df,b, left_on = ['index'], right_on = ['index'], how = 'left')
    
    # reformat census tract column
    foia_7a_df['_tract'] = foia_7a_df['_tract'].apply(float)/100
    foia_7a_df['_tract'] = foia_7a_df['_tract'].apply(format_census_tract)

    # map in county names 
    url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
    fips_dict = fcc_fips_mappings_getter(url)
    foia_7a_df['_county_fips_code'] = foia_7a_df['_state_fips_code'].apply(str) + foia_7a_df['_county_fips_code'].apply(str)
    foia_7a_df['_county_fips_code'] = foia_7a_df['_county_fips_code'].map(fips_dict['fcc_counties'])
    foia_7a_df['_state_fips_code'] = foia_7a_df['_state_fips_code'].map(fips_dict['fcc_states'])
    foia_7a_df = foia_7a_df.rename(columns = {'_county_fips_code':'_fips_county_name', '_state_fips_code':'_fips_state_name'})

    # create "county_final" column that will be based on address at default and zip codes county of NaN and then filter down to counties of focus using this new column
    foia_7a_df['county_final'] = np.where(foia_7a_df['_fips_county_name'].apply(str) == 'nan', foia_7a_df['County_from_Zipcode'], foia_7a_df['_fips_county_name'])
    foia_7a_df = foia_7a_df[(foia_7a_df['county_final'] == 'Tarrant County') | (foia_7a_df['county_final'] == 'Collin County') | (foia_7a_df['county_final'] == 'Dallas County')]

    return foia_7a_df
