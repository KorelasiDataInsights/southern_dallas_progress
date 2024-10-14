
import os
import pandas as pd
import numpy as np


# helper function to label affiliated banks with the same institution name
def affiliate_institutions(df):
    df['institution_name'] = df['institution_name'].str.strip()
    df['institution_name'] = df['institution_name'].replace({
        'American Bank, National Association':'American Bank, N.A.',
        'Amerifirst Financial, Inc.':'AMERIFIRST FINANCIAL CORPORATION',
        'ANGEL OAK MORTGAGE SOLUTIONS LLC':'ANGEL OAK HOME LOANS LLC',
        'Citibank, National Association':'Citibank, N.A.',
        'Citizens Bank, National Association':'Citizens Bank',
        'EECU':'Educational Employees Credit Union',
        'First Bank & Trust Co.':'First Bank & Trust',
        'First Financial Bank, National Association':'First Financial Bank',
        'GREYSTONE SERVICING COMPANY LLC':'GREYSTONE FUNDING COMPANY LLC',
        'GUARANTEED RATE, INC.':'GUARANTEED RATE AFFINITY, LLC',
        'Guaranty Bank and Trust Company':'Guaranty Bank & Trust, N.A.',
        'Guaranty Bank & Trust, National Association':'Guaranty Bank & Trust, N.A.',
        'LAKEVIEW LOAN SERVICING, LLC':'Lakeview Community Capital, LLC',
        'Meridian Bank Corporation':'Meridian Bank',
        'Peoples National Bank , N.A.':'Peoples Bank',
        'RB MORTGAGE LLC':'RANDOLPH-BROOKS',
        'Readycap Lending, LLC':'READYCAP COMMERCIAL, LLC',
        'RESIDENTIAL WHOLESALE MORTGAGE, INC.':'RESIDENTIAL HOME FUNDING CORP.',
        'SOFI LENDING CORP.':'SNB Bank, National Association',
        'SoFi Bank, National Association':'SNB Bank, National Association',
        'Texas Bank and Trust Company':'Texas Bank', 
        'TOWNE MORTGAGE COMPANY':'Towne Bank',
        'U.S. Bank, National Association':'U.S. Bank National Association',
        'Waterstone Mortgage Corporation':'WaterStone Bank, SSB',
        'Zions Bank, A Division of':'Zions Bancorporation, N.A.', # no entires of 'Zions Bank, A Division of' present/seen 
        # Mapping of post-processed CRA names
        'AMERICAN NATIONAL BANK': 'AMERICAN NATIONAL BANK OF TEXAS',                                                                           
        'CITIZENS BANK': 'CITIZENS BANK NA',                                                                                                   
        'FIRST BANK AND TRUST COMPANY': 'FIRST BANK & TRUST',                                                                                  
        'FIRST INTERNET BANK': 'FIRST INTERNET BANK OF INDIANA',                                               
        'FIRST MID BANK AND TRUST NA': 'FIRST MID BANK & TRUST NA',                                                                            
        'FIRST UNITED B&TC': 'FIRST UNITED BANK AND TRUST COMPANY',                                                                            
        'GUARANTY BANK & TRUST': 'GUARANTY BANK & TRUST NA',                                                                                   
        'GULF COAST BANK AND TRUST': 'GULF COAST BANK AND TRUST COMPANY',                                                                      
        'MORGAN STANLEY PRIVATE BANKNA': 'MORGAN STANLEY PRIVATE BANK NA',                                                                     
        'NORTHERN BANK & TRUST CO': 'NORTHERN BANK AND TRUST COMPANY',                                                                         
        'PRIMIS': 'PRIMIS MORTGAGE COMPANY',                                                                                                   
        'SOUTHERN BANCORP BANK': 'SOUTHERN BANK',                                                                                              
        'STEARNS BANK N A': 'STEARNS BANK NA',                                                                                                 
        'STIFEL BANK': 'STIFEL BANK AND TRUST',                                                                                                
        'STIFEL BANK & TRUST': 'STIFEL BANK AND TRUST',                                                                                        
        'THE NORTHERN TRUST CO': 'THE NORTHERN TRUST COMPANY',                                                                                 
        'WASHINGTON FEDERAL': 'WASHINGTON FEDERAL BANK NA'
        
        
        }) 
    return df



# helper function to clean institution names and make them more consistent 
def institution_name_transform(df):
    # remove leading and trailing white space
    df['institution_name'] = df['institution_name'].str.strip()
    # make all institution names upper case 
    df['institution_name'] = df['institution_name'].str.upper()
    # remove "." in "INC."
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace(".","")\
        if "INC" in x else x)
    # remove "." in "L.L.C."
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace(".","")\
        if "L.L.C." or "LLC." in x else x)
    # replace NATIONAL ASSOCIATION with NA 
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace("NATIONAL ASSOCIATION","NA")\
        if "NATIONAL ASSOCIATION" in x else x)
    # replace "N.A." with "NA"
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace("N.A.","NA")\
        if "N.A." in x else x)
    # replace "FEDERAL CREDIT UNION" with "FCU"
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace("FEDERAL CREDIT UNION","FCU")\
        if "FEDERAL CREDIT UNION" in x else x)
    # replace "CREDIT UNION" with CU
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace("CREDIT UNION","CU")\
        if "CREDIT UNION" in x else x)
    df['institution_name'] = df['institution_name'].apply(\
        lambda x: x.replace(",","")\
        if "," in x else x)    
    return df



def clean_inst_names(df):
    df = affiliate_institutions(df)
    df = institution_name_transform(df)
    df = affiliate_institutions(df)
    return df


def export_to_excel(df, out_file_path, sheet_name="data", startcol=0, startrow=0):
    print(f"Exporting dataframe to {out_file_path}")  
    with pd.ExcelWriter(out_file_path) as writer:
        # Insert the dataframe
        df.to_excel(writer, sheet_name=sheet_name, startcol=startcol, startrow=startrow) 




def bin_category(df:pd.core.frame.DataFrame, 
                 cols_to_check:list, 
                 threshold:int, 
                 normalize_inpt:bool = False, 
                 cnt_or_prp:str = 'count',
                 verbose:bool = False) -> pd.core.frame.DataFrame:
    for category_col in cols_to_check:
        temp_df = df[category_col].value_counts(normalize = normalize_inpt).reset_index()
        # Rename second column to cnt_or_prp
        temp_df = temp_df.rename(columns = {'index':category_col, category_col:cnt_or_prp})
        if verbose: 
            print(temp_df.head(5))
        replace_map = {val_under_threshold:'Other' for val_under_threshold in temp_df[temp_df[cnt_or_prp] < threshold][category_col]}
        if len(replace_map) > 1:
            df[category_col] = df[category_col].replace(replace_map)
    return df






