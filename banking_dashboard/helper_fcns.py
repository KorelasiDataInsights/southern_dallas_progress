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



def ffiec_flat_file_extractor(url:str)-> Dataset[str,str,str,...,str]:
    """
    Used to extract csv files from ffiec website and convert into pandas dataframe.
    
    Args:
        url: The url path to the zip file on the ffiec website.
    
    Returns: 
        A dataframe of the downloaded and transformed zip file
    
    Raises: 
        TypeError: if n is not a string.
    """
    
    # code to ingest url from website
    filename = wget.download(url)
    zip_ref = zipfile.ZipFile(filename, 'r')
    current_dir = os.getcwd()
    unzipped = zip_ref.extractall(current_dir)
    CensusFlatFile2022 = pd.read_csv(url)
    #transformations to be added
    #--
    
    return CensusFlatFile2022



def hdma_helper_fcn(url:str): 
    
    """Used to locate and download all zip files on hdma 2022 page.
    
    Args: 
        url: url of HDMA page with zip file datasets on it. 
        
    Returns:
        A dictionary of dataframes. One for each ingested csv file from the HDMA website.
        
    Raises:
        TypeError: if url is not a string.
    """
    
    # NOTE: page associated with HDMA url uses javascript so http request made will need to match http request being made from
    # javascript(https://stackoverflow.com/questions/26393231/using-python-requests-with-javascript-pages). Additionally,
    # the extracted Loan/Application Records (LAR) csv is greater than 5 gigabytes so working to find other method 
    # of storing file. When analyzing the LAR dataset, using the dask library will work specificlly dask.dataframe().
    
    snpshot_page_html = requests.get(url2)  
    snpshot_page_bs = BeautifulSoup(snpshot_page_html.text, 'html') # convert result from requests to BeautifulSoup object
    list_of_datafile_urls = snpshot_page_bs.find_all("ul") # return all ul tags as a list('ul' tag is location of datafiles)
    dict_of_dfs = {}
    for datafile_url in list_of_datafiles_urls: 
        if "csv.zip" in datafile_url: # only want csv zip files
            filename = wget.download(datafile_url)
            zip_ref = zipfile.ZipFile(filename, 'r')
            current_dir = os.getcwd()
            zip_ref.extractall(current_dir) # extract zip file contents to current directory
            dict_of_dfs[file] = pd.read_csv(filename, nrows = 10) #only read in 10 rows to start
    return dict_of_dfs
    


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



