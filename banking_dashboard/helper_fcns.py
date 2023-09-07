# the following are the helper functions that will be used to ingest the data from sources in Aappendix A

import requests
import bs4
import json
import os
import pandas as pd
from io import StringIO
import wget
import zipfile


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
    
    # code to ingest from url to be added
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



