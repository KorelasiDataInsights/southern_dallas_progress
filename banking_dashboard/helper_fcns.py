# the following are the helper functions that will be used to ingest the data from sources in Aappendix A

import requests
from bs4 import BeautifulSoup
import wget 
import zipfile 
import json
import os
import pandas as pd
import requests
import re
from datetime import datetime

# census tract helper function
def census_data_ingester(file_name:str) :
    """"Takes in url and downloads csv from census tract website. The downloaded csv is then read in as a dataframe 
    and transformed into a format that can be used for join on future tract years where 'tract','county',and 'state'
    are the composite primary key.
        
    Args:
        file_name: The name of the downloaded csv file from the census tract website.
        
    Returns:
         A dataframe of the downloaded and transformed zip file
         
    Raises: 
        TypeError: if n is not a string.
    """
    
    # code to ingest from url to be added(work being done on census website)
    #---
    
    # transform ingested data
    data = pd.read_csv(file_name)
    data = data.T
    data = data.reset_index()
    data.columns = data.iloc[0]
    data['test1'] = data['Label (Grouping)'].str.split(',').to_frame()
    data[['tract','county','state']] = pd.DataFrame(data['test1'].to_list(), columns = ['tract','county','state'])[['tract','county','state']]
    data = data.drop(data.index[0])
    data = data.drop(columns = ['test1'])#, axis = 1)
    data = data.drop(columns = ['Label (Grouping)'])#, axis = 1)
    data = data.set_index(['tract','county','state'])
    data.columns = list(data.columns.str.replace(u'\xa0', u' ').str.replace(':','').str.lstrip(' ')) # remove \xa0 Latin1 characters and ":" in column names
    data = data.replace('[^0-9.]', '', regex = True) # replace commas in entry values with nothing 
    data = data.apply(pd.to_numeric,downcast = 'float') #convert all count values to floats for later calculations 
    return data

# ffiec helper function
def ffiec_flat_file_extractor(file:str, data_dict:str)->pd.core.frame.DataFrame:
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
    data_dictionary = pd.read_excel(data_dict, sheet_name = 'Data Dictionary')
    data_dictionary = data_dictionary[data_dictionary['Index']>=0]
    new_ffiec_cols = data_dictionary['Description']
    data = pd.read_csv(file, nrows = 8000, header = None)
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
    alphanum_to_str_dict = {an_field:str for an_field in alphanumeric_field_list} 
    data = data.astype(alphanum_to_str_dict) # casting aplhanumeric fields to strings
    numeric_field_list = list(data.loc[:,~data.columns.isin(alphanumeric_field_list)].columns)
    numeric_to_float_dict = {n_field:float for n_field in numeric_field_list} 
    data = data.astype(numeric_to_float_dict) # casting numeric fields to floats 
    return data

# hmda helper function
def hmda_data_ingester(url:str)->dict[pd.core.frame.DataFrame]:
    
    """Used to read in all necessary .csv files from HMDA website and return a dictionary containing all of the read in
    files.
    
    Args: 
        url: url of HMDA page with zip file datasets on it. 
        
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
    lar_df = pd.read_csv('2022_public_lar_csv.csv', nrows = 50000)
    # mapping values for columns in Loan/Application Records(LAR)

    lar_df['conforming_loan_limit'] = lar_df['conforming_loan_limit'].map({"C (Conforming)":"Conforming",
                                                   "NC (Nonconforming)":"Nonconforming",
                                                   "U (Undetermined)":"Undetermined",
                                                   "NA (Not Applicable)":"Not Applicable"})
    
    lar_df['action_taken'] = lar_df['action_taken'].map({1:"Loan originated",
                                                         2:"Application approved but not accepted",
                                                         3:"Application denied",
                                                         4:"Application withdrawn by applicant",
                                                         5:"File closed for incompleteness",
                                                         6:"Purchased loan",
                                                         7:"Preapproval request denied",
                                                         8:"Preapproval request approved but not accepted"})

    lar_df['purchaser_type'] = lar_df['purchaser_type'].map({0:"Not applicable",
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
    
    lar_df['preapproval'] = lar_df['preapproval'].map({1:"Preapproval requested",
                                                       2:"Preapproval not requested"})

    
    lar_df['loan_type'] = lar_df['loan_type'].map({1:"Conventional (not insured or guaranteed by FHA, VA, RHS, or FSA)",
                                                   2:"Federal Housing Administration insured (FHA)",
                                                   3:"Veterans Affairs guaranteed (VA)",
                                                   4:"USDA Rural Housing Service or Farm Service Agency guaranteed (RHS or FSA)"})
    
    lar_df['loan_purpose'] = lar_df['loan_purpose'].map({1:"Home purchase",
                                                         2:"Home improvement",
                                                         31:"Refinancing",
                                                         32:"Cash-out refinancing",
                                                         4:"Other purpose",
                                                         5:"Not applicable"})

    lar_df['lien_status'] = lar_df['lien_status'].map({1:"Secured by a first lien",
                                                       2:"Secured by a subordinate lien"})  

    lar_df['reverse_mortgage'] = lar_df['reverse_mortgage'].map({1:"Reverse mortgage",
                                                                 2:"Not a reverse mortgage",
                                                                 1111:"Exempt"})  
    
    lar_df['open_end_line_of_credit'] = lar_df['open_end_line_of_credit'].map({1:"Open-end line of credit",
                                                                               2:"Not an open-end line of credit",
                                                                               1111:"Exempt"})  

    lar_df['business_or_commercial_purpose'] = lar_df['business_or_commercial_purpose'].map({1:"Primarily for a business or commercial purpose",
                                                                                             2:"Not primarily for a business or commercial purpose",
                                                                                             1111:"Exempt"})

    lar_df['hoepa_status'] = lar_df['hoepa_status'].map({1:"High-cost mortgage",
                                                         2:"Not a high-cost mortgage",
                                                         3:"Not applicable"})

    lar_df['negative_amortization'] = lar_df['negative_amortization'].map({1:"Negative amortization",
                                                                           2:"No negative amortization",
                                                                           1111:"Exempt"})

    lar_df['interest_only_payment'] = lar_df['interest_only_payment'].map({1:"Interest-only payments",
                                                                           2:"No interest-only payments",
                                                                           1111:"Exempt"})

    lar_df['balloon_payment'] = lar_df['balloon_payment'].map({1:"Balloon payment",
                                                               2:"No balloon payment",
                                                               1111:"Exempt"})

    lar_df['other_nonamortizing_features'] = lar_df['other_nonamortizing_features'].map({1:"Other non-fully amortizing features",
                                                                                         2:"No other non-fully amortizing features",
                                                                                         1111:"Exempt"})

    lar_df['construction_method'] = lar_df['construction_method'].map({1:"Site-built",
                                                                       2:"Manufactured home"})

    lar_df['occupancy_type'] = lar_df['occupancy_type'].map({1:"Principal residence",
                                                             2:"Second residence",
                                                             3:"Investment property"})

    lar_df['manufactured_home_secured_property_type'] = lar_df['manufactured_home_secured_property_type'].map({1:"Manufactured home and land",
                                                                                                               2:"Manufactured home and not land",
                                                                                                               3:"Not applicable",
                                                                                                               1111:"Exempt"})

    lar_df['manufactured_home_land_property_interest'] = lar_df['manufactured_home_land_property_interest'].map({1:"Direct ownership",
                                                                                                                 2:"Indirect ownership",
                                                                                                                 3:"Paid leasehold",
                                                                                                                 4:"Unpaid leasehold",
                                                                                                                 5:"Not applicable",
                                                                                                                 1111:"Exempt"})

    lar_df['applicant_credit_score_type'] = lar_df['applicant_credit_score_type'].map({1:"Equifax Beacon 5.0",
                                                                                       2:"Experian Fair Isaac",
                                                                                       3:"FICO Risk Score Classic 04",
                                                                                       4:"FICO Risk Score Classic 98",
                                                                                       5:"VantageScore 2.0",
                                                                                       6:"VantageScore 3.0",
                                                                                       7:"More than one credit scoring model",
                                                                                       8:"Other credit scoring model",
                                                                                       9:"Not applicable",
                                                                                       1111:"Exempt"})

    lar_df['co_applicant_credit_score_type'] = lar_df['co_applicant_credit_score_type'].map({1:"Equifax Beacon 5.0",
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

    lar_df['applicant_ethnicity_1'] = lar_df['applicant_ethnicity_1'].map({1:"Hispanic or Latino",
                                                                           11:"Mexican",
                                                                           12:"Puerto Rican",
                                                                           13:"Cuban",
                                                                           14:"Other Hispanic or Latino",
                                                                           2:"Not Hispanic or Latino",
                                                                           3:"Information not provided by applicant in mail, internet, or telephone application",
                                                                           4:"Not applicable"})

    lar_df['applicant_ethnicity_2'] = lar_df['applicant_ethnicity_2'].map({1:"Hispanic or Latino",
                                                                            11:"Mexican",
                                                                            12:"Puerto Rican",
                                                                            13:"Cuban",
                                                                            14:"Other Hispanic or Latino",
                                                                            2:"Not Hispanic or Latino"})

    lar_df['applicant_ethnicity_3'] = lar_df['applicant_ethnicity_3'].map({1:"Hispanic or Latino",
                                                                        11:"Mexican",
                                                                        12:"Puerto Rican",
                                                                        13:"Cuban",
                                                                        14:"Other Hispanic or Latino",
                                                                        2:"Not Hispanic or Latino"})

    lar_df['applicant_ethnicity_4'] = lar_df['applicant_ethnicity_4'].map({1:"Hispanic or Latino",
                                                                            11:"Mexican",
                                                                            12:"Puerto Rican",
                                                                            13:"Cuban",
                                                                            14:"Other Hispanic or Latino",
                                                                            2:"Not Hispanic or Latino"})

    lar_df['applicant_ethnicity_5'] = lar_df['applicant_ethnicity_5'].map({1:"Hispanic or Latino",
                                                                            11:"Mexican",
                                                                            12:"Puerto Rican",
                                                                            13:"Cuban",
                                                                            14:"Other Hispanic or Latino",
                                                                            2:"Not Hispanic or Latino"})

    lar_df['co_applicant_ethnicity_1'] = lar_df['co_applicant_ethnicity_1'].map({1:"Hispanic or Latino",
                                                                                 11:"Mexican",
                                                                                 12:"Puerto Rican",
                                                                                 13:"Cuban",
                                                                                 14:"Other Hispanic or Latino",
                                                                                 2:"Not Hispanic or Latino",
                                                                                 3:"Information not provided by applicant in mail, internet, or telephone application",
                                                                                 4:"Not applicable",
                                                                                 5:"No co-applicant"})

    lar_df['co_applicant_ethnicity_2'] = lar_df['co_applicant_ethnicity_2'].map({1:"Hispanic or Latino",
                                                                                 11:"Mexican",
                                                                                 12:"Puerto Rican",
                                                                                 13:"Cuban",
                                                                                 14:"Other Hispanic or Latino",
                                                                                 2:"Not Hispanic or Latino"})

    lar_df['co_applicant_ethnicity_3'] = lar_df['co_applicant_ethnicity_3'].map({1:"Hispanic or Latino",
                                                                                 11:"Mexican",
                                                                                 12:"Puerto Rican",
                                                                                 13:"Cuban",
                                                                                 14:"Other Hispanic or Latino",
                                                                                 2:"Not Hispanic or Latino"})

    lar_df['co_applicant_ethnicity_4'] = lar_df['co_applicant_ethnicity_4'].map({1:"Hispanic or Latino",
                                                                                 11:"Mexican",
                                                                                 12:"Puerto Rican",
                                                                                 13:"Cuban",
                                                                                 14:"Other Hispanic or Latino",
                                                                                 2:"Not Hispanic or Latino"})

    lar_df['co_applicant_ethnicity_5'] = lar_df['co_applicant_ethnicity_5'].map({1:"Hispanic or Latino",
                                                                                 11:"Mexican",
                                                                                 12:"Puerto Rican",
                                                                                 13:"Cuban",
                                                                                 14:"Other Hispanic or Latino",
                                                                                 2:"Not Hispanic or Latino"})

    lar_df['applicant_ethnicity_observed'] = lar_df['applicant_ethnicity_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                         2:"Not collected on the basis of visual observation or surname",
                                                                                         3:"Not applicable"})

    lar_df['co_applicant_ethnicity_observed'] = lar_df['co_applicant_ethnicity_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                               2:"Not collected on the basis of visual observation or surname",
                                                                                               3:"Not applicable",
                                                                                               4:"No co-applicant"})

    lar_df['applicant_race_1'] = lar_df['applicant_race_1'].map({1:"American Indian or Alaska Native",
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
    
    lar_df['applicant_race_2'] = lar_df['applicant_race_2'].map({1:"American Indian or Alaska Native",
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
    
    lar_df['applicant_race_3'] = lar_df['applicant_race_3'].map({1:"American Indian or Alaska Native",
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
    
    lar_df['applicant_race_4'] = lar_df['applicant_race_4'].map({1:"American Indian or Alaska Native",
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
    
    lar_df['applicant_race_5'] = lar_df['applicant_race_5'].map({1:"American Indian or Alaska Native",
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

    lar_df['co_applicant_race_1'] = lar_df['co_applicant_race_1'].map({1:"American Indian or Alaska Native",
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

    lar_df['co_applicant_race_2'] = lar_df['co_applicant_race_2'].map({1:"American Indian or Alaska Native",
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

    lar_df['co_applicant_race_3'] = lar_df['co_applicant_race_3'].map({1:"American Indian or Alaska Native",
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

    lar_df['co_applicant_race_4'] = lar_df['co_applicant_race_4'].map({1:"American Indian or Alaska Native",
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

    lar_df['co_applicant_race_5'] = lar_df['co_applicant_race_5'].map({1:"American Indian or Alaska Native",
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

    lar_df['applicant_race_observed'] = lar_df['applicant_race_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                               2:"Not collected on the basis of visual observation or surname",
                                                                               3:"Not applicable"})

    lar_df['co_applicant_race_observed'] = lar_df['co_applicant_race_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                     2:"Not collected on the basis of visual observation or surname",
                                                                                     3:"Not applicable",
                                                                                     4:"No co-applicant"})

    lar_df['applicant_sex'] = lar_df['applicant_sex'].map({1:"Male",
                                                           2:"Female",
                                                           3:"Information not provided by applicant in mail, internet, or telephone application",
                                                           4:"Not applicable",
                                                           6:"Applicant selected both male and female"})

    lar_df['co_applicant_sex'] = lar_df['co_applicant_sex'].map({1:"Male",
                                                                 2:"Female",
                                                                 3:"Information not provided by applicant in mail, internet, or telephone application",
                                                                 4:"Not applicable",
                                                                 5:"No co-applicant",
                                                                 6:"Co-applicant selected both male and female"})

    lar_df['applicant_sex_observed'] = lar_df['applicant_sex_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                             2:"Not collected on the basis of visual observation or surname",
                                                                             3:"Not applicable"})

    lar_df['co_applicant_sex_observed'] = lar_df['co_applicant_sex_observed'].map({1:"Collected on the basis of visual observation or surname",
                                                                                   2:"Not collected on the basis of visual observation or surname",
                                                                                   3:"Not applicable",
                                                                                   4:"No co-applicant"})

    lar_df['submission_of_application'] = lar_df['submission_of_application'].map({1:"Submitted directly to your institution",
                                                                                   2:"Not submitted directly to your institution",
                                                                                   3:"Not applicable",
                                                                                   1111:"Exempt"})

    lar_df['initially_payable_to_institution'] = lar_df['initially_payable_to_institution'].map({1:"Initially payable to your institution",
                                                                                                 2:"Not initially payable to your institution",
                                                                                                 3:"Not applicable",
                                                                                                 1111:"Exempt"})
    lar_df['aus_1'] = lar_df['aus_1'].map({1:"Desktop Underwriter (DU)",
                                           2:"Loan Prospector (LP) or Loan Product Advisor",
                                           3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                           4:"Guaranteed Underwriting System (GUS)",
                                           5:"Other",
                                           6:"Not applicable",
                                           7:"Internal Proprietary System",
                                           1111:"Exempt"})

    lar_df['aus_2'] = lar_df['aus_2'].map({1:"Desktop Underwriter (DU)",
                                       2:"Loan Prospector (LP) or Loan Product Advisor",
                                       3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                       4:"Guaranteed Underwriting System (GUS)",
                                       5:"Other",
                                       7:"Internal Proprietary System"})

    lar_df['aus_3'] = lar_df['aus_3'].map({1:"Desktop Underwriter (DU)",
                                       2:"Loan Prospector (LP) or Loan Product Advisor",
                                       3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                       4:"Guaranteed Underwriting System (GUS)",
                                       7:"Internal Proprietary System"})

    lar_df['aus-4'] = lar_df['aus_4'].map({1:"Desktop Underwriter (DU)",
                                       2:"Loan Prospector (LP) or Loan Product Advisor",
                                       3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                       4:"Guaranteed Underwriting System (GUS)",
                                       7:"Internal Proprietary System"})

    lar_df['aus_5'] = lar_df['aus_5'].map({1:"Desktop Underwriter (DU)",
                                       2:"Loan Prospector (LP) or Loan Product Advisor",
                                       3:"Technology Open to Approved Lenders (TOTAL) Scorecard",
                                       4:"Guaranteed Underwriting System (GUS)",
                                       7:"Internal Proprietary System"})

    lar_df['denial_reason_1'] = lar_df['denial_reason_1'].map({1:"Debt-to-income ratio",
                                                               2:"Employment history",
                                                               3:"Credit history",
                                                               4:"Collateral",
                                                               5:"Insufficient cash (downpayment, closing costs)",
                                                               6:"Unverifiable information",
                                                               7:"Credit application incomplete",
                                                               8:"Mortgage insurance denied",
                                                               9:"Other",
                                                               10:"Not applicable"})

    lar_df['denial_reason_2'] = lar_df['denial_reason_2'].map({1:"Debt-to-income ratio",
                                                               2:"Employment history",
                                                               3:"Credit history",
                                                               4:"Collateral",
                                                               5:"Insufficient cash (downpayment, closing costs)",
                                                               6:"Unverifiable information",
                                                               7:"Credit application incomplete",
                                                               8:"Mortgage insurance denied",
                                                               9:"Other"})

    lar_df['denial_reason_3'] = lar_df['denial_reason_3'].map({1:"Debt-to-income ratio",
                                                               2:"Employment history",
                                                               3:"Credit history",
                                                               4:"Collateral",
                                                               5:"Insufficient cash (downpayment, closing costs)",
                                                               6:"Unverifiable information",
                                                               7:"Credit application incomplete",
                                                               8:"Mortgage insurance denied",
                                                               9:"Other"})

    lar_df['denial_reason_4'] = lar_df['denial_reason_4'].map({1:"Debt-to-income ratio",
                                                               2:"Employment history",
                                                               3:"Credit history",
                                                               4:"Collateral",
                                                               5:"Insufficient cash (downpayment, closing costs)",
                                                               6:"Unverifiable information",
                                                               7:"Credit application incomplete",
                                                               8:"Mortgage insurance denied",
                                                               9:"Other"})

    # recast data types
    lar_df = lar_df.astype({"derived_msa_md":str, 
                   "census_tract":str})
    
    # read in transmittal sheet records as df
    ts_df = pd.read_csv("2022_public_ts_csv.csv")
    
    # replacing values of "agency_code" with actual string fields in transmittal sheet dataset
    ts_df['agency_code'] = ts_df['agency_code'].map({1:"Office of the Comptroller of the Currency",
                                                     2:"Federal Reserve System",
                                                     3:"Federal Deposit Insurance Corporation",
                                                     5:"National Credit Union Administration",
                                                     7:"Department of Housing and Urban Development",
                                                     9:"Consumer Financial Protection Bureau"})

    # read in reporter panel data as df
    panel_df = pd.read_csv('2022_public_panel_csv.csv', na_values = [-1]) # -1 is being encoded for NULL so I am replacing 
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
    # renaming upper field to lei
    panel_df.rename(columns = {'upper':'lei'}, inplace = True)
    
    # read in metropolitan statistical area and metropolitan division data as df
    msamd_df = pd.read_csv('2022_public_msamd_csv.csv') # nothing written in data dictionary saying 99999 is na but it does
                                                        # not look like a legitamate msa_md code

    # recast data types
    msamd_df = msamd_df.astype({"msa_md":str})
    
        
    # arid_2017 = pd.read_csv('arid2017_to_lei_xref_csv.csv') # not using for the moment because not joining in previous 
                                                              # years so do not need to use
        
    hmda_dict = {"lar_df":lar_df,"ts_df":ts_df, "panel_df":panel_df, "msamd_df":msamd_df}

    
    
    return hmda_dict

# cra helper function      
def cra_data_ingester(file:str)->dict[str:pd.core.frame.DataFrame]:
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
    
    fwf_dimensions_dict = {'cra2021_Aggr_A11.dat':[a_1_1_widths,a_1_1_fields],
                           'cra2021_Aggr_A11a.dat':[a_1_1a_widths,a_1_1a_fields],
                           'cra2021_Aggr_A12.dat':[a_1_2_widths,a_1_2_fields],
                           'cra2021_Aggr_A12a.dat':[a_1_2a_widths,a_1_2a_fields],
                           'cra2021_Aggr_A21.dat':[a_2_1_widths,a_2_1_fields],
                           'cra2021_Aggr_A21a.dat':[a_2_1a_widths,a_2_1a_fields],
                           'cra2021_Aggr_A22.dat':[a_2_2_widths,a_2_2_fields],
                           'cra2021_Aggr_A22a.dat':[a_2_2a_widths,a_2_2a_fields],
                           'cra2021_Discl_D11.dat':[d_1_1_widths,d_1_1_fields],
                           'cra2021_Discl_D12.dat':[d_1_2_widths,d_1_2_fields],
                           'cra2021_Discl_D21.dat':[d_2_1_widths,d_2_1_fields],
                           'cra2021_Discl_D22.dat':[d_2_2_widths,d_2_2_fields],
                           'cra2021_Discl_D3.dat':[d3_widths,d3_fields],
                           'cra2021_Discl_D4.dat':[d4_widths,d4_fields],
                           'cra2021_Discl_D5.dat':[d5_widths,d5_fields],
                           'cra2021_Discl_D6.dat':[d6_widths,d6_fields]}
    
    df_dict = {}
    for i in os.listdir():
        if i in fwf_dimensions_dict: 
            df_dict[i] = pd.read_fwf(i, widths = fwf_dimensions_dict[i][0], header = None, names = fwf_dimensions_dict[i][1])
    return df_dict
    
def zero_adder(fips_code:str)->str:
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

def fcc_fips_mappings_getter(url:str)->dict[str:dict[str:str]]:
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

def cra_mapping_function(df_dictionary:dict[str:pd.core.frame.DataFrame])->dict[str:pd.core.frame.DataFrame]:
    """Used to map full descriptions to data entires that use codes as place holders in cra data.
    
    Args:
        df_dictionary: a dictionary of dataframes reulting from the cra_data_ingester function.
    
    Returns: 
        A dictionary of cra data dataframes where the .dat cra data file name is the key and the corresponding dataframe is the value.
    """
    # A11
    df_dictionary['cra2021_Aggr_A11.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A11.dat']['Loan Type'].map({
        4:"Small Business", 
        })
    
    df_dictionary['cra2021_Aggr_A11.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A11.dat']['Action Taken Type'].map({
        1:"Originations"
    })
    
    # df['State'].map({
    # }).replace(np.nan, "totals")
    
    # df['County'].map({
    # }).replace(np.nan, "totals")

    df_dictionary['cra2021_Aggr_A11.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A11.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A11.dat']['Census Tract'] = df_dictionary['cra2021_Aggr_A11.dat']['Census Tract'].replace(np.nan, "totals")
    
    df_dictionary['cra2021_Aggr_A11.dat']['Split County Indicator'] = df_dictionary['cra2021_Aggr_A11.dat']['Split County Indicator'].map({
        "Y":"YES",
        "N":"NO"
    }).replace(np.nan, "total")
    
    df_dictionary['cra2021_Aggr_A11.dat']['Population Classification'] = df_dictionary['cra2021_Aggr_A11.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with > 500,000 in population"
    }).replace(np.nan, "total")
    
    df_dictionary['cra2021_Aggr_A11.dat']['Income Group Total'] = df_dictionary['cra2021_Aggr_A11.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Aggr_A11.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A11.dat']['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")
    
    #A11a
    df_dictionary['cra2021_Aggr_A11a.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A11a.dat']['Loan Type'].map({
        4:"Small Business", 
    })
    
    df_dictionary['cra2021_Aggr_A11a.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A11a.dat']['Action Taken Type'].map({
        1:"Originations"
    })

    # df['State'].map({
    # }).replace(np.nan, "total")
    
    # df['County'].map({
    # }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A11a.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A11a.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A11a.dat']['Respondent ID'] = df_dictionary['cra2021_Aggr_A11a.dat']['Respondent ID'].replace(np.nan, "total")
    
    df_dictionary['cra2021_Aggr_A11a.dat']['Agency Code'] = df_dictionary['cra2021_Aggr_A11a.dat']['Agency Code'].map({
        1:"OCC",
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A11a.dat']['Number of Lenders'] = df_dictionary['cra2021_Aggr_A11a.dat']['Number of Lenders'].replace(np.nan, "not a total")

    df_dictionary['cra2021_Aggr_A11a.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A11a.dat']['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")


    #A12
    df_dictionary['cra2021_Aggr_A12.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A12.dat']['Loan Type'].map({
        4:"Small Business"
    })

    df_dictionary['cra2021_Aggr_A12.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A12.dat']['Action Taken Type'].map({
        6:"Purchases"
    })

    # df['State'].map({
    # }).replace(np.nan, "total")
    
    # df['County'].map({
    # }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A12.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A12.dat']['Census Tract'] = df_dictionary['cra2021_Aggr_A12.dat']['Census Tract'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12.dat']['Split County Indicator'] = df_dictionary['cra2021_Aggr_A12.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12.dat']['Population Classification'] = df_dictionary['cra2021_Aggr_A12.dat']['Population Classification'].map({
       "S":"counties with < or = to 500,000 in population",
       "L":"counties with > 500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12.dat']['Income Group Total'] = df_dictionary['cra2021_Aggr_A12.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Aggr_A12.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A12.dat']['Report Level'].map({
       100:"Income Group Total",
       200:"County Total",
       210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A12a 
    df_dictionary['cra2021_Aggr_A12a.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A12a.dat']['Loan Type'].map({
        4:"Small Business", 
    })
    
    df_dictionary['cra2021_Aggr_A12a.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A12a.dat']['Action Taken Type'].map({
        6:"Purchases"
    })

    # df['State'].map({
    # }).replace(np.nan, "total")
    
    # df['County'].map({
    # }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12a.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A12a.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A12a.dat']['Respondent ID'] = df_dictionary['cra2021_Aggr_A12a.dat']['Respondent ID'].replace(np.nan, "total")
    
    df_dictionary['cra2021_Aggr_A12a.dat']['Agency Code'] = df_dictionary['cra2021_Aggr_A12a.dat']['Agency Code'].map({
        1:"OCC",
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12a.dat']['Number of Lenders'] = df_dictionary['cra2021_Aggr_A12a.dat']['Number of Lenders'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A12a.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A12a.dat']['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A21
    df_dictionary['cra2021_Aggr_A21.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A21.dat']['Loan Type'].map({
        5:"Small Farm"        
    })

    df_dictionary['cra2021_Aggr_A21.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A21.dat']['Action Taken Type'].map({
        1:"Originations"        
    })

    # df['State'].map({
    # }).replace(np.nan, "total")
    
    # df['County'].map({
    # }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A21.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A21.dat']['Census Tract'] = df_dictionary['cra2021_Aggr_A21.dat']['Census Tract'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21.dat']['Split County Indicator'] = df_dictionary['cra2021_Aggr_A21.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"       
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21.dat']['Population Classification'] = df_dictionary['cra2021_Aggr_A21.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with > 500,000 in population" 
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21.dat']['Income Group Total'] = df_dictionary['cra2021_Aggr_A21.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Aggr_A21.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A21.dat']['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A21a 
    df_dictionary['cra2021_Aggr_A21a.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A21a.dat']['Loan Type'].map({
        5:"Small Farm" 
    })

    df_dictionary['cra2021_Aggr_A21a.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A21a.dat']['Action Taken Type'].map({
        1:"Originations"        
    })

    # df['State'].map({
    # }).replace(np.nan, "total")
    
    # df['County'].map({
    # }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21a.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A21a.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")
    
    df_dictionary['cra2021_Aggr_A21a.dat']['Respondent ID'] = df_dictionary['cra2021_Aggr_A21a.dat']['Respondent ID'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21a.dat']['Agency Code'] = df_dictionary['cra2021_Aggr_A21a.dat']['Agency Code'].map({
        1:"OCC",
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21a.dat']['Number of Lenders'] = df_dictionary['cra2021_Aggr_A21a.dat']['Number of Lenders'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A21a.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A21a.dat']['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A22
    df_dictionary['cra2021_Aggr_A22.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A22.dat']['Loan Type'].map({
         5:"Small Farm"
     })

    df_dictionary['cra2021_Aggr_A22.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A22.dat']['Action Taken Type'].map({
        6:"Purchases"
    })

    # df['State'].map({
    # }).replace(np.nan, "not a total")
    
    # df['County'].map({
    # }).replace(np.nan, "not a total")

    df_dictionary['cra2021_Aggr_A22.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A22.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A22.dat']['Census Tract'] = df_dictionary['cra2021_Aggr_A22.dat']['Census Tract'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A22.dat']['Split County Indicator'] = df_dictionary['cra2021_Aggr_A22.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A22.dat']['Population Classification'] = df_dictionary['cra2021_Aggr_A22.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with > 500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A22.dat']['Income Group Total'] = df_dictionary['cra2021_Aggr_A22.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Aggr_A22.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A22.dat']['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # A22a
    df_dictionary['cra2021_Aggr_A22a.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A22a.dat']['Loan Type'].map({
        5:"Small Farm"
    })

    df_dictionary['cra2021_Aggr_A22a.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A22a.dat']['Action Taken Type'].map({
        6:"Purchases"
    })

    # df['State'].map({
    # }).replace(np.nan, "total")
    
    # df['County'].map({
    # }).replace(np.nan, "total")
    df_dictionary['cra2021_Aggr_A22a.dat']['MSA/MD'] = df_dictionary['cra2021_Aggr_A22a.dat']['MSA/MD'].replace(np.nan, "area outside of an MSA/MD")

    df_dictionary['cra2021_Aggr_A22a.dat']['Respondent ID'] = df_dictionary['cra2021_Aggr_A22a.dat']['Respondent ID'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A22a.dat']['Agency Code'] = df_dictionary['cra2021_Aggr_A22a.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A22a.dat']['Number of Lenders'] = df_dictionary['cra2021_Aggr_A22a.dat']['Number of Lenders'].replace(np.nan, "total")

    df_dictionary['cra2021_Aggr_A22a.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A22a.dat']['Report Level'].map({
        200:"County Total",
        210:"MSA/MD Total"
    }).replace(np.nan, "not a total")

    # D11
    df_dictionary['cra2021_Discl_D11.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D11.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D11.dat']['Loan Type'] =  df_dictionary['cra2021_Discl_D11.dat']['Loan Type'].map({
        4:"Small Business"
    })

    df_dictionary['cra2021_Discl_D11.dat']['Action Taken Type'] =  df_dictionary['cra2021_Discl_D11.dat']['Action Taken Type'].map({
        1:"Originations"
    })

     # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D11.dat']['MSA/MD'] =  df_dictionary['cra2021_Discl_D11.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D11.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D11.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary['cra2021_Discl_D11.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D11.dat']['Partial County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D11.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D11.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D11.dat']['Population Classification'] =  df_dictionary['cra2021_Discl_D11.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D11.dat']['Income Group Total'] =  df_dictionary['cra2021_Discl_D11.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Discl_D11.dat']['Report Level'] =  df_dictionary['cra2021_Discl_D11.dat']['Report Level'].map({
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
    df_dictionary['cra2021_Discl_D12.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D12.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D12.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D12.dat']['Agency Code'].map({
        4:"Small Business"
    })

    df_dictionary['cra2021_Discl_D12.dat']['Action Taken Type'] =  df_dictionary['cra2021_Discl_D12.dat']['Action Taken Type'].map({
        6:"Purchases"
    })

    # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D12.dat']['MSA/MD'] =  df_dictionary['cra2021_Discl_D12.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D12.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D12.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary['cra2021_Discl_D12.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D12.dat']['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D12.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D12.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D12.dat']['Population Classification'] =  df_dictionary['cra2021_Discl_D12.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D12.dat']['Income Group Total'] =  df_dictionary['cra2021_Discl_D12.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Discl_D12.dat']['Report Level'] =  df_dictionary['cra2021_Discl_D12.dat']['Report Level'].map({
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
    df_dictionary['cra2021_Discl_D21.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D21.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D21.dat']['Loan Type'] =  df_dictionary['cra2021_Discl_D21.dat']['Loan Type'].map({
       5:"Small Farm"
    })

    df_dictionary['cra2021_Discl_D21.dat']['Action Taken Type'] =  df_dictionary['cra2021_Discl_D21.dat']['Action Taken Type'].map({
       1:"Originations"
    })

    # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D21.dat']['MSA/MD'] =  df_dictionary['cra2021_Discl_D21.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D21.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D21.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary['cra2021_Discl_D21.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D21.dat']['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D21.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D21.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D21.dat']['Population Classification'] =  df_dictionary['cra2021_Discl_D21.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D21.dat']['Income Group Total'] =  df_dictionary['cra2021_Discl_D21.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Discl_D21.dat']['Report Level'] =  df_dictionary['cra2021_Discl_D21.dat']['Report Level'].map({
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
    df_dictionary['cra2021_Discl_D22.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D22.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D22.dat']['Loan Type'] =  df_dictionary['cra2021_Discl_D22.dat']['Loan Type'].map({
       5:"Small Farm"
    })

    df_dictionary['cra2021_Discl_D22.dat']['Action Taken Type'] =  df_dictionary['cra2021_Discl_D22.dat']['Action Taken Type'].map({
       6:"Purchases"
    })

    # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D22.dat']['MSA/MD'] =  df_dictionary['cra2021_Discl_D22.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D22.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D22.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary['cra2021_Discl_D22.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D22.dat']['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D22.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D22.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D22.dat']['Population Classification'] =  df_dictionary['cra2021_Discl_D22.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population", 
        "L":"counties with >500,000 in population"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D22.dat']['Income Group Total'] =  df_dictionary['cra2021_Discl_D22.dat']['Income Group Total'].map({
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

    df_dictionary['cra2021_Discl_D22.dat']['Report Level'] =  df_dictionary['cra2021_Discl_D22.dat']['Report Level'].map({
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
    df_dictionary['cra2021_Discl_D3.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D3.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D3.dat']['Loan Type'] =  df_dictionary['cra2021_Discl_D3.dat']['Loan Type'].map({
       4:"Small Business"
    })

    # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D3.dat']['MSA/MD'] = df_dictionary['cra2021_Discl_D3.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D3.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D3.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary['cra2021_Discl_D3.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D3.dat']['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D3.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D3.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D22.dat']['Report Level'] =  df_dictionary['cra2021_Discl_D22.dat']['Report Level'].map({
        5:"Assessment Area Total",
        10:"County Total within Assessment Area",
        15:"Activity Inside all Assessment Areas",
        20:"Activity Outside Assessment Area(s)",
        30:"Total Loans (Inside +Outside Assessment Area)"
    })

    #D4
    df_dictionary['cra2021_Discl_D4.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D4.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D4.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D4.dat']['Agency Code'].map({
      5:"Small Farm"
    })

    # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D4.dat']['MSA/MD'] = df_dictionary['cra2021_Discl_D4.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D4.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D4.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area (including predominately military areas)")

    df_dictionary['cra2021_Discl_D4.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D4.dat']['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D4.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D4.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    }).replace(np.nan, "total")

    df_dictionary['cra2021_Discl_D22.dat']['Report Level'] =  df_dictionary['cra2021_Discl_D22.dat']['Report Level'].map({
        5:"Assessment Area Total",
        10:"County Total within Assessment Area",
        15:"Activity Inside all Assessment Areas",
        20:"Activity Outside Assessment Area(s)",
        30:"Total Loans (Inside +Outside Assessment Area)"
    })

    #D5
    df_dictionary['cra2021_Discl_D5.dat']['Loan Type'] =  df_dictionary['cra2021_Discl_D5.dat']['Loan Type'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    })

    df_dictionary['cra2021_Discl_D5.dat']['Loan Type'] =  df_dictionary['cra2021_Discl_D5.dat']['Loan Type'].map({
        6:"Community Development",
        7:"Consortium/Third-Party"
    })

    df_dictionary['cra2021_Discl_D5.dat']['Action Type'] =  df_dictionary['cra2021_Discl_D5.dat']['Action Type'].map({
        "O":"Originated",
        "P":"Purchased",
        "T":"Total (Originated + Purchased)"
    })
    
    #D6
    df_dictionary['cra2021_Discl_D6.dat']['Agency Code'] =  df_dictionary['cra2021_Discl_D6.dat']['Agency Code'].map({
        1:"OCC", 
        2:"FRS",
        3:"FDIC",
        4:"OTS"
    }).replace(np.nan, "total")

    # df['State'].map({
    # })
    
    # df['County'].map({
    # })

    df_dictionary['cra2021_Discl_D6.dat']['MSA/MD'] = df_dictionary['cra2021_Discl_D6.dat']['MSA/MD'].replace(np.nan, "area outside of MSA/MD")

    df_dictionary['cra2021_Discl_D6.dat']['Assessment Area Number'] =  df_dictionary['cra2021_Discl_D6.dat']['Assessment Area Number'].replace(\
        np.nan, "area outside of an Assessment Area(s) (including predominately military areas)")

    df_dictionary['cra2021_Discl_D6.dat']['Partial County Indicator'] =  df_dictionary['cra2021_Discl_D6.dat']['Partial County Indicator'].map({
        "Y":"Yes", 
        "N":"No"
    })

    df_dictionary['cra2021_Discl_D6.dat']['Split County Indicator'] =  df_dictionary['cra2021_Discl_D6.dat']['Split County Indicator'].map({
        "Y":"Yes",
        "N":"No"
    })

    df_dictionary['cra2021_Discl_D6.dat']['Population Classification'] =  df_dictionary['cra2021_Discl_D6.dat']['Population Classification'].map({
        "S":"counties with < or = to 500,000 in population",
        "L":"counties with >500,000 in population"
    })

    df_dictionary['cra2021_Discl_D6.dat']['Population Classification'] =  df_dictionary['cra2021_Discl_D6.dat']['Population Classification'].map({
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

    df_dictionary['cra2021_Discl_D6.dat']['Loan Indicator'] =  df_dictionary['cra2021_Discl_D6.dat']['Loan Indicator'].map({
        "Y":"Yes",
        "N":"No"
    })

    return df_dictionary

def state_county_fips_mapper(df_dict:dict[pd.core.frame.DataFrame],fcc_fips_dict:dict[str:dict[str:str]])->dict[str:pd.core.frame.DataFrame]:
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

def thousands_adder(df_dict:dict[str:pd.core.frame.DataFrame])->dict[str:pd.core.frame.DataFrame]:
    """" Multiplies all fields in cra data that contain total loan amounts by 1000.
        
    Args:
        df_dict: A dictionary of cra dataframes reulting from the state_county_fips_mapper function. 
        
    Returns:
         A dictionary of cra dataframes with loan amount columns showing their full amount(e.g. 153 is not 153000)
         """
    for file_name in df_dict.keys():
        for column in df_dict[file_name].columns:
            if 'Total Loan Amount'in column:
                #print(file_name, column)
                df_dict[file_name][column] = df_dict[file_name][column]*1000 
    return df_dict

# fdic helper function
def changec_label_adder(file_name:str)->dict[str:str]:
    """Used to create dictionary of old column names as the key and new column names as the value using the institutions definitions file.
    
    Args: 
        file_name: Name of institutions definitions data file
        
    Returns:
        a dictionary of old column names as the keys and new column names as the values
        """
    institutions_definitions_df = pd.read_csv(file_name)
    col_name_replace_map = dict(zip(institutions_definitions_df['Variable Name'],institutions_definitions_df['Variable Label']))  
    # loop through the dictionary create between the variable name and variable label fields to add numbers to distinguish the CHANGEC
    # values
    for original_field in col_name_replace_map.keys():
        if "CHANGEC" in original_field: 
            col_name_replace_map[original_field] = col_name_replace_map[original_field] + " " + original_field.split('CHANGEC')[1]
    return col_name_replace_map     

def fdic_institutions_ingester(institutions_file_name:str, col_replace_map:dict[str:str])->pd.core.frame.DataFrame:
    """Used to read in institution data for those created on or before 12/31/2023 and are in dallas, collins or tarrant county.
    
    Args: 
        institutions_file_name: Name of institutions data file
        col_replace_map: dictionary containing columns names returned from changec_label_adder() function.
        
    Returns:
        A dataframe of the fdic institutions data
        """
    institutions_df = pd.read_csv(institutions_file_name)
    institutions_df.rename(columns = col_replace_map, inplace = True)
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
    institutions_df = institutions_df.astype({'Federal Reserve ID Number':str})
    # institutions_df['Federal Reserve ID Number'] = institutions_df['Federal Reserve ID Number'].map({"1":"Boston",
    #                                                                                                  "2":"New York",
    #                                                                                                  "3":"Philadelphia",
    #                                                                                                  "4":"Cleveland",
    #                                                                                                  "5":"Richmond",
    #                                                                                                  "6":"Atlanta",
    #                                                                                                  "7":"Chicago",
    #                                                                                                  "8":"St. Louis",
    #                                                                                                  "9":"Minneapolis",
    #                                                                                                  "10":"Kansas city",
    #                                                                                                  "11":"Dallas", 
    #                                                                                                  "12":"San Francisco"})
    institutions_df['Primary Regulator'] = institutions_df['Primary Regulator'].map({"OCC":"Office of the Comptroller of Currency",
                                                                                     "FDIC":"Federal Deposit Insurance Corporation",
                                                                                     "FRB":"Federal Reserve Board",
                                                                                     "NCUA":"National Credit Union Association",
                                                                                     "OTS":"Office of Thrift Supervision"})
    # institutions_df['Supervisory Region Number'] = institutions_df['Supervisory Region Number'].map({"02":"New York",
    #                                                                                                  "05":"Atlanta",
    #                                                                                                  "09":"Chicago",
    #                                                                                                  "11":"Kansas City",
    #                                                                                                  "13"::"Dallas",
    #                                                                                                  "14":"San Francisco",
    #                                                                                                  "16":"Office of Complex Financial Institutions (CFI)"})
    # institutions_df['Trust Powers'] = institutions_df['Trust Powers'].map({"00":"Trust Powers Not Known",
    #                                                                        "10":"Full Trust Powers Granted",
    #                                                                        "11":"Full Trust Powers Granted, Exercised",
    #                                                                        "12":"Full Trust Powers Granted, Not Exercised",
    #                                                                        "20":"Limited Trust Powers Granted",
    #                                                                        "21":"Limited Trust Powers Granted, Exercised",
    #                                                                        "30":"Trust Powers Not Granted",
    #                                                                        "31":"Trust Powers Not Granted, But Exercised",
    #                                                                        "40":"Trust Powers Grandfathered"})
    institutions_df['State Charter'] = institutions_df['State Charter'].map({1:"yes",0:"no"})
    institutions_df['FFIEC Call Report 31 Filer'] = institutions_df['FFIEC Call Report 31 Filer'].map({1:"yes",0:"no"})
    institutions_df['Bank Holding Company Type'] = institutions_df['Bank Holding Company Type'].map({1:"yes", 0:"no"})
    institutions_df['Deposit Insurance Fund member'] = institutions_df['Deposit Insurance Fund member'].map({1:"Yes", 0:"No"})
    institutions_df['Law Sasser Flag'] = institutions_df['Law Sasser Flag'].map({1:"Yes", 0:"No"})
    institutions_df = institutions_df.astype({'Credit Card Institutions':int}, errors = 'ignore')
    # filter for established in 12/31/2022 or before
    institutions_df['Established Date'] = pd.to_datetime(institutions_df['Established Date'])
    institutions_df[institutions_df['Established Date'] <= '2022-12-31']
    # filter for dallas, collins and tarrant counties in TX
    institutions_df = institutions_df[institutions_df['State Alpha code'] == 'TX']
    institutions_df = institutions_df[(institutions_df['County'] == 'Tarrant') | (institutions_df['County'] == 'Collin') | (institutions_df['County'] == 'Dallas')]
    return institutions_df

def fdic_locations_mapper(locations_def_file:str, locations_file:str)->pd.core.frame.DataFrame:
    """Used to read in locations data for those created on or before 12/31/2023 and are in dallas, collins or tarrant county.
    
    Args: 
        locations_file: Name of locations data file
        locations_def_file: Name of locations definitions file
        
    Returns:
        A dataframe of of the fdic locations data
    """ 
    loc_fed_df = pd.read_csv(fdic_locations_definitions)
    bkclass_replace_map = dict(zip(loc_fed_df.iloc[2:8,:]['TITLE'].str.replace(' ','').str.strip('-'), loc_fed_df.iloc[2:8,:]['DEFINITION']))
    serve_type_map = dict(zip(loc_fed_df.iloc[31:47,:]['TITLE'],loc_fed_df.iloc[31:47,:]['DEFINITION']))
    inst_col_name_map = dict(zip(loc_fed_df[loc_fed_df['NAME'].notnull()]['NAME'], loc_fed_df[loc_fed_df['NAME'].notnull()]['TITLE']))
    fdic_locations_df = pd.read_csv(fdic_locations)
    fdic_locations_df['BKCLASS'] = fdic_locations_df['BKCLASS'].map(bkclass_replace_map)
    fdic_locations_df['SERVTYPE'] = fdic_locations_df['SERVTYPE'].map(serve_type_map)
    fdic_locations_df.rename(columns = inst_col_name_map, inplace = True)
    # map descriptions to columns using codes as place holders
    fdic_locations_df['Metropolitan Divisions Flag (Branch)'] = fdic_locations_df['Metropolitan Divisions Flag (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Metropolitan Division Flag (Branch)'] = fdic_locations_df['Metropolitan Division Flag (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Micropolitan Division Flag (Branch)'] = fdic_locations_df['Micropolitan Division Flag (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Combined Statistical Area Flag  (Branch)'] = fdic_locations_df['Combined Statistical Area Flag  (Branch)'].map({1:"Yes",0:"No"})
    fdic_locations_df['Branch Established Date'] = pd.to_datetime(fdic_locations_df['Branch Established Date'])
    fdic_locations_df = fdic_locations_df[fdic_locations_df['Branch Established Date'] <= '2022-12-31']
    # filter for location in texas and counties that are dallas, collins or tarrant
    fdic_locations_df = fdic_locations_df[fdic_locations_df['Branch State   '] == 'Texas']
    final_fdic_locations_df = fdic_locations_df[(fdic_locations_df['Branch County'] == 'Tarrant') | (fdic_locations_df['Branch County'] == 'Collin') | (fdic_locations_df['Branch County'] == 'Dallas')]
    return final_fdic_locations_df