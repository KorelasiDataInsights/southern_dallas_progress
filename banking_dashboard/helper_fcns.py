# the following are the helper functions that will be used to ingest the data from sources in Aappendix A

import requests
from bs4 import BeautifulSoup
import wget 
import zipfile 
import json
import os
import pandas as pd
from io import StringIO

def census_data_ingester(url:str):
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
                      "Meets at least one of current or previous year's CRA distressed/underserved tract criteria? 'X' - Yes, ' ' (blank space) - No":"Meets at least one of current or previous year's CRA distressed/underserved tract criteria?"}, inplace = True)

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
    ts_df = pd.read_csv("2022_public_ts_csv.csv")
    
    # replacing values of "agency_code" with actual string fields
    ts_df['agency_code'] = ts_df['agency_code'].map({1:"Office of the Comptroller of the Currency",
                                                     2:"Federal Reserve System",
                                                     3:"Federal Deposit Insurance Corporation",
                                                     5:"National Credit Union Administration",
                                                     7:"Department of Housing and Urban Development",
                                                     9:"Consumer Financial Protection Bureau"})
    
    panel_df = pd.read_csv('2022_public_panel_csv.csv', na_values = [-1]) # -1 is being encoded for NULL so I am replacing 
                                                                          # -1 with NaN. No description in data dictionary for 
                                                                          # field called "upper"
            
    # replacing values of "other_lender_code" with actual string fields
    panel_df['other_lender_code'] = panel_df['other_lender_code'].map({0:"Depository Institution",
                                                                       1:"MBS of state member bank",
                                                                       2:"MBS of bank holding company",
                                                                       3:"Independent mortgage banking subsidiary",
                                                                       5:"Affiliate of a depository institution"}) 
    
    # replacing values of "agency_code" with actual string fields
    panel_df['agency_code'] = panel_df['agency_code'].map({1:"Office of the Comptroller of the Currency",
                                                           2:"Federal Reserve System",
                                                           3:"Federal Deposit Insurance Corporation",
                                                           5:"National Credit Union Administration",
                                                           7:"Department of Housing and Urban Development",
                                                           9:"Consumer Financial Protection Bureau"})
    
    msamd_df = pd.read_csv('2022_public_msamd_csv.csv') # nothing written in data dictionary saying 99999 is na but it does
                                                        # not look like a legitamate msa_md code
        
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

    url = 'https://www.ffiec.gov/cra/xls/21exp_aggr.zip'
    r = requests.get(url, allow_redirects = True)
    open('21exp_aggr.zip','wb').write(r.content)
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
        
    
    
