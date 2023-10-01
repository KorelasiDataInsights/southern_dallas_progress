# the following are the helper functions that will be used to ingest the data from sources in Aappendix A

import requests
from bs4 import BeautifulSoup
import wget 
import zipfile 
import json
import os
import pandas as pd
from io import StringIO

def census_data_ingester(url:str = "url", file_name:str = "file") :
    """"
    Takes in url and downloads csv from census tract website. The downloaded csv is then read in as a dataframe 
    and transformed into a format that can be used for join on future tract years where 'tract','county',and 'state'
    are the composite primary key.
        
    Args:
        url:  The url path to the csv file on the census tract website.
        
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
    data = data.drop(columns = ['test1'])
    data = data.drop(columns = ['Label (Grouping)'])
    data = data.set_index(['tract','county','state'])
    data.columns = list(data.columns.str.replace(u'\xa0', u' ').str.replace(':','').str.lstrip(' ')) # remove \xa0 Latin1 characters and ":" in column names
    data = data.replace('[^0-9.]', '', regex = True) # replace commas in entry values with nothing 
    data = data.apply(pd.to_numeric,downcast = 'float') #convert all count values to floats for later calcukations 
    return data


def ffiec_flat_file_extractor(file_url:str, data_dict_url:str)->pd.core.frame.DataFrame:
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
    data_dictionary = pd.read_excel('FFIEC_Census_File_Definitions_26AUG22.xlsx', sheet_name = 'Data Dictionary')
    data_dictionary = data_dictionary[data_dictionary['Index']>=0]
    new_ffiec_cols = data_dictionary['Description']
    data = pd.read_csv('CensusFlatFile2022.csv', nrows = 8000, header = None)
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
    data = data.astype({"HMDA/CRA collection year":str,
                        "MSA/MD Code":str,
                        "FIPS state code":str,
                        "FIPS county code":str,
                        "Census tract. Implied decimal point":str,
                        "Principal city flag":str,
                        "Small county flag":str,
                        "Split tract flag":str,
                        "Demographic data flag":str,
                        "Urban/rural flag":str,
                        "CRA poverty criteria":str,
                        "CRA unemployment criteria":str,
                        "CRA distressed criteria":str,
                        "CRA remote rural (low density) criteria":str,
                        "Previous year CRA distressed criteria":str,
                        "Previous year CRA underserved criterion":str,
                        "Meets at least one of current or previous year's CRA distressed/underserved tract criteria?":str})

    an_fields = ["HMDA/CRA collection year",
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
    data.loc[:,~data.columns.isin(an_fields)].loc[:] = data.loc[:,~data.columns.isin(an_fields)].astype(int, errors = 'ignore').loc[:]

    return data

# hdma helper function
def hdma_data_ingester(url:str)->dict[pd.core.frame.DataFrame]:
    
    """Used to read in all necessary .csv files from HDMA website and return a dictionary containing all of the read in
    files.
    
    Args: 
        url: url of HDMA page with zip file datasets on it. 
        
    Returns:
        A dictionary of dataframes. One for each ingested file from the HDMA website.
        
    Raises:
        TypeError: if url is not a string.
    """
    
    # NOTE: page associated with url uses javascript so http request made will need to match http request being made from
    # javascript(https://stackoverflow.com/questions/26393231/using-python-requests-with-javascript-pages). Additionally,
    # the extracted Loan/Application Records (LAR) csv is greater than 5 gigabytes so will working to find other method 
    # of storing file. When analyzing the LAR dataset, using the dask library will work specificlly dask.dataframe().
    
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

    ts_df = pd.read_csv("2022_public_ts_csv.csv")
    
    # replacing values of "agency_code" with actual string fields in transmittal sheet dataset
    ts_df['agency_code'] = ts_df['agency_code'].map({1:"Office of the Comptroller of the Currency",
                                                     2:"Federal Reserve System",
                                                     3:"Federal Deposit Insurance Corporation",
                                                     5:"National Credit Union Administration",
                                                     7:"Department of Housing and Urban Development",
                                                     9:"Consumer Financial Protection Bureau"})

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
    
    msamd_df = pd.read_csv('2022_public_msamd_csv.csv') # nothing written in data dictionary saying 99999 is na but it does
                                                        # not look like a legitamate msa_md code

    # recast data types
    msamd_df = msamd_df.astype({"msa_md":str})
    
        
    # arid_2017 = pd.read_csv('arid2017_to_lei_xref_csv.csv') # not using for the moment because not joining in previous 
                                                              # years
        
    hdma_dict = {"lar_df":lar_df,"ts_df":ts_df, "panel_df":panel_df, "msamd_df":msamd_df}
    return hdma_dict
      

def hdma_data_merger(hdma_dict_ipt:dict[pd.core.frame.DataFrame])->pd.core.frame.DataFrame:
    
    """Takes in hdma dictionary of dataframes, merges each dataframe in the dictionary, and returns merged dictionaries.
    
    Args: 
        url: url of HDMA page with zip file datasets on it. 
        
    Returns:
        A dictionary of dataframes. One for each ingested file from the HDMA website.
        
    Raises:
        TypeError: if input is not a dictionary of dataframes is not a string.
    """
    lar_ts = pd.merge(hdma_dict_ipt["lar_df"], hdma_dict_ipt["ts_df"], how = 'inner') # unique identifier: lei(legal entity identifier)
    lar_ts_panel = pd.merge(lar_ts, hdma_dict_ipt["panel_df"], how = 'inner') # unique identifier: tax_id
    lar_ts_panel_msamd = pd.merge(lar_ts_panel, hdma_dict_ipt["msamd_df"],left_on = ['derived_msa_md'], right_on = ['msa_md'], how = 'inner') # unique identifiers: derived_msa_md, msa_md

    return lar_ts_panel_msamd

# Potential future process 
# Automatically download datafiles by providing main page url and names of clickable links that lead to page with downloadable files
# on it. Use Selenium to automate clicks needed to land on page with downloadable files then download the files using above function.

# Parameters in function:

# main_page_url (str) : url of main page
# click_1 (str) : name of clickable link on page 
# click_2 (str) : name of clickable link on page
# click_3 (str) : name of clickable link on page
# path_for_download_file (str) : url path for downloadable file i.e. zip file etc

# Example
# 
# main_page_url = https://www.consumerfinance.gov/data-research/hmda/
# str: click_1 = "See recent data and summaries"
# str: click_2 = "Snapshot National Loan-Level Dataset"
# str: click_3 = 2022
# str: path_for_download_file = https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2022/2022_public_lar_csv.zip



def cra_data_ingester(url:str)-> dict[pd.core.frame.DataFrame]:

    """Used to read in all necessary zipfiles from ffiec website, unzip them and read in all .dat files.
    
    Args: 
        url: url of CRA zip file. 
        
    Returns:
        A dictionary of dataframes. One for each ingested file from the CRA zip file.
        
    Raises:
        TypeError: if url is not a string.
    """

    #url = 'https://www.ffiec.gov/cra/xls/21exp_aggr.zip'
    #r = requests.get(url, allow_redirects = True)
    #open('21exp_aggr.zip','wb').write(r.content)
    #zip_ref = zipfile.ZipFile('21exp_aggr.zip', 'r') #zipfile not zip file error

    #def fixed width file mappings
    a_1_1_fields  = ["Table ID","Activity Year", "Loan Type", " Action Taken Type", "State", "County", "MSA/MD", "Census Tract", 
    "Split County Indicator", "Population Classification", "Income Group Total", "Report Level",
    " Number of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000", 
    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000",
    "Number of Small Business Loans Originated with Loan Amount at Origination > 100,000 and < or = to $250,000",
    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $100,000 and < or = to $250,000",
    "Number of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000", 
    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000" ,
    " Number of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million",
    "Total Loan Amount of Loans Originated to Small Businesses with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    a_1_1_widths = [5,4,1,1,2,3,5,7,1,1,3,3,10,10,10,10,10,10,10,10,29]
    
    a_1_1a_fields = ["Table ID","Activity Year", "Loan Type", " Action Taken Type", "State", "County", "MSA/MD", "Respondent ID", "Agency Code",
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
    
    
    a_1_2a_fields = ["Table ID","Activity Year", "Loan Type", " Action Taken Type", "State", "County", "MSA/MD", "Respondent ID",
    " Agency Code", " Number of Lenders", "Report Level", "Number of Small Business Loans", "Total Loan Amount of Small Business Loans", 
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
    
    
    a_2_1a_fields = ["Table ID","Activity Year", "Loan Type", " Action Taken Type", "State", "County", "MSA/MD", "Respondent ID", " Agency Code", 
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
    
    a_2_2a_fields = ["Table ID","Activity Year", "Loan Type", " Action Taken Type", "State", "County", "MSA/MD", "Respondent ID", " Agency Code", 
    "Number of Lenders", "Report Level", "Number of Small Farm Loans", "Total Loan Amount of Small Farm Loans",
    "Number of loans to Small Farms with Gross Annual Revenues < or = to $1 million", 
    "Total Loan Amount of loans to Small Farms with Gross Annual Revenues < or = to $1 million", "Filler"]
    
    
    a_2_2a_widths = [5,4,1,1,2,3,5,10,1,5,3,10,10,10,10,65]
    
    
    d_1_1_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "Action Taken Type", " State", "County", "MSA/MD", 
                    "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Population Classification", 
                    "Income Group Total", "Report Level",
                    "Number of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000", 
                    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination < or = to $100,000", 
                    "Number of Small Business Loans Originated with Loan Amount at Origination > 100,000 and < or = to $250,000",
                    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $100,000 and < or = to $250,000", 
                    "Number of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000", 
                    "Total Loan Amount of Small Business Loans Originated with Loan Amount at Origination > $250,000 and < or = to $1,000,000",
                    " Number of Loans Originated to Small Businesses with Gross Annual Revenues < $1 million", 
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
          " Number of Small Business Loans Purchased", "Total Loan Amount of Small Business Loans Purchased", "Filler"]
    
    d3_widths = [5,10,1,4,1,2,3,5,4,1,1,2,10,10,10,10,10,10,46]
    
    
    
    d4_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", "State", "County", "MSA/MD", 
          "Assessment Area Number", "Partial County Indicator", "Split County Indicator", "Report Level",
          "Number of Small Farm Loans Originated", "Total Loan Amount of Small Farm Loans Originated",
          "Number of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million", 
          "Total Loan Amount of Loans Originated to Small Farms with Gross Annual Revenues < or = to $1 million",
          " Number of Small Farm Loans Purchased", "Total Loan Amount of Small Farm Loans Purchased", "Filler"]
    
    
    d4_widths = [5,10,1,4,1,2,3,5,4,1,1,2,10,10,10,10,10,10,46]
    
    
    d5_fields = ["Table ID","Respondent ID", "Agency Code", "Activity Year", "Loan Type", " Number of Loans",
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
                           'cra2021_Aggr_D11.dat':[d_1_1_widths,d_1_1_fields],
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
    


def cra_mapping_function(df_dictionary:dict[pd.core.frame.DataFrame])->dict[pd.core.frame.DataFrame]:
    """Used to map full descriptions to columns of dataframes in cra dictionary of dataframes.
    
    Args: 
        df_dictionary: url of CRA zip file. 
        
    Returns:
        A modified dictionary of dataframes.
        
    Raises:
        TypeError: if df_dictionary is not a dictionary.
    """
    # A11
    df_dictionary['cra2021_Aggr_A11.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A11.dat']['Loan Type'].map({
        4:"Small Business", 
        })
    
    df_dictionary['cra2021_Aggr_A11.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A11.dat']['Action Taken Type'].map({
        1:"Originations"
    })
    
    # df['State'].map({
    # })
    
    # df['County'].map({
    # })
    
    df_dictionary['cra2021_Aggr_A11.dat']['Split County Indicator'] = df_dictionary['cra2021_Aggr_A11.dat']['Split County Indicator'].map({
        "Y":"YES",
        "N":"NO"
    }).replace(np.nan, "blank for totals")
    
    df_dictionary['cra2021_Aggr_A11.dat']['Population Classification'] = df_dictionary['cra2021_Aggr_A11.dat']['Population Classification'].map({
        "S":"counties with< 500,000 in population",
         "L":"counties with>500,000 in population"
    }).replace(np.nan, "blank for totals")
    
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
        13:"> 120% of MFI",
        14:"MFI not known (income percentage = 0)",
        15:"Tract not Known (reported as NA)",
        101:"Low Income (< 50% of MFI - excluding 0)",
        102:"Moderate Income (50% to 80% of MFI)",
        103:"Middle Income (80% to 120% of MFI)",
        104:"Upper Income (> 120% of MFI)",
        105:"Income Not Known (0)",
        106:"Tract not Known (NA)"
    })
    #A11a
    df_dictionary['cra2021_Aggr_A11.dat']['Report Level'] = df_dictionary['cra2021_Aggr_A11.dat']['Report Level'].map({
        100:"Income Group Total",
        200:"County Total",
        210:"MSA/MD Total"
    })
    
    
    # ['cra2021_Aggr_A11a.dat']
    df_dictionary['cra2021_Aggr_A11a.dat']['Loan Type'] = df_dictionary['cra2021_Aggr_A11a.dat']['Loan Type'].map({
        4:"Small Business", 
        })
    
    df_dictionary['cra2021_Aggr_A11a.dat']['Action Taken Type'] = df_dictionary['cra2021_Aggr_A11a.dat']['Action Taken Type'].map({
        1:Originations
    })


    return df_dictionary
    
    
