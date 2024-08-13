import pandas as pd
import re
import logging
import json
def compare_dataframes(df1, df2):

    df2=df2.astype(str)
    df1=df1.astype(str)
    df2=df2[['URL', 'Company']]
    # Fusionner les deux DataFrames en utilisant une jointure externe pour obtenir toutes les lignes de df1
    merged_df = pd.merge(df1, df2, on=['URL', 'Company'], how='left', indicator=True)
    
    # Conserver uniquement les lignes présentes dans df1 mais pas dans df2
    result_df = merged_df[merged_df['_merge'] == 'left_only']

    # Supprimer la colonne d'indicateur '_merge'
    result_df.drop('_merge', axis=1, inplace=True)

    return result_df
 
def duplicates_linkedin(df1_original, df2_original):
    df1=df1_original[['First Name', 'Last Name', 'URL', 'Company','Position']]
    df2=df2_original[['First Name', 'Last Name', 'URL', 'Company','Position']]
    df2_original=df2_original.astype(str)
    df1[['First Name', 'Last Name', 'URL', 'Company','Position']] = df1[['First Name', 'Last Name', 'URL', 'Company','Position']].astype(str)
    df2[['First Name', 'Last Name', 'URL', 'Company','Position']] = df2[['First Name', 'Last Name', 'URL',  'Company','Position']].astype(str)
    
    df1['First Name'] = df1['First Name'].astype(str).apply(lambda x: re.sub(r'[^a-zA-ZàâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ-]', '', x).title())
    df1['Last Name'] = df1['Last Name'].astype(str).apply(lambda x: re.sub(r'[^a-zA-ZàâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ-]', '', x).title())
    df2['First Name'] = df2['First Name'].astype(str).apply(lambda x: re.sub(r'[^a-zA-ZàâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ-]', '', x).title())
    df2['Last Name'] = df2['Last Name'].astype(str).apply(lambda x: re.sub(r'[^a-zA-ZàâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ-]', '', x).title())
 
    merged = pd.merge(df2, df1, how='left', indicator=True)
 
    df2_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
 
    print("Nombre de lignes à ajouter :")
    print(len(df2_filtered))
 
    # df2_filtered['is_duplicate'] = df2_filtered['Company'].isin(df1['Company']).astype(bool)
    # percentage_duplicate = df2_filtered['is_duplicate'].sum() / len(df2_filtered) * 100
    # print("Pourcentage d'entreprises dans df2 qui sont présentes dans df1: {:.2f}%".format(percentage_duplicate))
    # dfconcat = pd.concat([df1, df2_filtered], axis=0)
    # print("Nombre de lignes restantes dans df1 avant suppression:")
    # dftotal = dfconcat.drop_duplicates(subset=['First Name', 'Last Name'], keep='last')
    # print(len(dftotal))
    # print("Nombre de lignes supprimées dans df1 après vérification changement d'entreprise :")
    # print(len(dfconcat) - len(dftotal))
    df2_original1=df2_original[['URL','Email Address','Position Group']]
    df2 = df2_filtered.merge(df2_original1, on="URL", how='inner')
    return df2

def compare_dataframes_outlook(data1, data2):
    logging.info("ca merge")
    #logging.info(data1)
    #logging.info(data2)
    #data1 = json.loads(data1)
    #data2 = json.loads(data2)
    if isinstance(data1, str):
        data1 = json.loads(data1)
    if isinstance(data2, str):
        data2 = json.loads(data2)
    merged_data = {}
    for company, details in data1.items():
        if company not in merged_data:
            merged_data[company] = details
        else:
            existing_domains = {detail['domain']: detail for detail in merged_data[company]}
            for detail in details:
                existing_domains[detail['domain']] = detail
            merged_data[company] = list(existing_domains.values())

    for company, details in data2.items():
        if company not in merged_data:
            merged_data[company] = details
        else:
            existing_domains = {detail['domain']: detail for detail in merged_data[company]}
            for detail in details:
                existing_domains[detail['domain']] = detail
            merged_data[company] = list(existing_domains.values())
    return merged_data

def duplicates_outlook(df1, df2):
    df1[['First Name', 'Last Name', 'Emails', 'Company Names', 'Phone Numbers']] = df1[['First Name', 'Last Name', 'Emails', 'Company Names', 'Phone Numbers']].astype(str)
    df2[['First Name', 'Last Name', 'Emails', 'Company Names', 'Phone Numbers']] = df2[['First Name', 'Last Name', 'Emails', 'Company Names', 'Phone Numbers']].astype(str)
 
    merged = pd.merge(df2, df1, how='left', indicator=True)
 
    df2_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
 
    print("Nombre de lignes à ajouter :")
    print(len(df2_filtered))
 
    if len(df2_filtered) > 0:
        df2_filtered['is_duplicate'] = df2_filtered['Company Names'].isin(df1['Company Names']).astype(bool)
        percentage_duplicate = df2_filtered['is_duplicate'].sum() / len(df2_filtered) * 100
        print("Pourcentage d'entreprises dans df2 qui sont présentes dans df1: {:.2f}%".format(percentage_duplicate))
 
    dfconcat = pd.concat([df1, df2_filtered], axis=0)
    print(len(dfconcat))
    print("Nombre de lignes restantes dans df1 avant suppression:")
    dftotal = dfconcat.drop_duplicates(subset=['First Name', 'Last Name'], keep='last')
    print(len(dftotal))
    print("Nombre de lignes supprimées dans df1 après vérification changement d'entreprise :")
    print(len(dfconcat) - len(dftotal))
    return df2_filtered

