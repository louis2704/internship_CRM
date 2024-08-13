import pandas as pd
from unidecode import unidecode


def convention_(df_mail):
    # List of personal email domains to exclude
    personal_email_domains = ['icloud', 'hotmail', 'gmail', 'yahoo']
    companies = df_mail['Company Names'].str.lower().unique()
    # Supprimer les lignes où la colonne 'Emails' est vide ou contient une liste vide
    df_mail = df_mail[~df_mail['Emails'].astype(str).eq('[]')]
    # Réinitialiser les index après la suppression
    df_mail.reset_index(drop=True, inplace=True)
    conventions = {}
    for company in companies:
        company_conventions = []  # Liste des conventions pour cette entreprise
        df_company = df_mail[df_mail['Company Names'].str.lower().eq(company)]
        for index, row in df_company.iterrows():
            if not pd.isnull(row['Last Name']) and not pd.isnull(row['First Name']):
                last_name = row['Last Name'].lower()
                first_name = row['First Name'].lower()
                for email in str(row['Emails']).split(','):
                
                    email = email.replace("'", "").replace("[", "").replace("]", "")

                    dns = email.split('@')[1]
                    left_part = email.split('@')[0].lower()
                    if dns.split('.')[0].replace("'","") not in personal_email_domains:
                        if '.' in left_part:
                            element_1 = left_part.split('.')[0].lower()  # Convertir en minuscules
                            element_2 = left_part.split('.')[1].lower()  # Convertir en minuscules
                            
                            if element_1 == first_name and element_2 == last_name:
                                if not any(c['convention'] == 'first.last'  and c['domain'] == dns for c in company_conventions) : #vérifier que la convention n'existe pas déjà
                                    company_conventions.append({'domain': dns, 'convention': 'first.last'})
                                    
                            elif len(element_1) == 1 and element_1.lower() == first_name[0] and element_2 == last_name:
                                if not any(c['convention'] == 'f.last' and c['domain'] == dns  for c in company_conventions)  : #vérifier que la convention n'existe pas déjà
                                    company_conventions.append({'domain': dns, 'convention': 'f.last'})
                            elif element_2 != last_name and  last_name in element_2:
                                element_3=element_2.split(last_name)[1]
                                element_2=last_name
                                if element_1 == first_name:
                                    if not any(c['convention'] == 'first.last' + element_3 and c['domain'] == dns for c in company_conventions):

                                        company_conventions.append({'domain': dns, 'convention': 'first.last'+element_3})
                                elif len(element_1) == 1 and element_1.lower() == first_name[0]:
                                    if not any(c['convention'] == 'f.last'+element_3 and c['domain'] == dns for c in company_conventions) : #vérifier que la convention n'existe pas déjà
                                        company_conventions.append({'domain': dns, 'convention': 'f.last'+element_3})


                        else:
                            if last_name in left_part:
                                element_1 = left_part.split(last_name)[0]
                                element_2 = last_name
                                element_3=''
                                if len(left_part.split(last_name))>1:
                                    element_3 = left_part.split(last_name)[1]
                                if element_1 == first_name and element_2 == last_name:
                                    if element_3 == '':
                                        if not any(c['convention'] == 'firstlast' and c['domain'] == dns  for c in company_conventions) : #vérifier que la convention n'existe pas déjà
                                            company_conventions.append({'domain': dns, 'convention': 'firstlast'})
                                    else:
                                        if not any(c['convention'] == 'firstlast' + element_3 and c['domain'] == dns for c in company_conventions) : #vérifier que la convention n'existe pas déjà
                                            company_conventions.append({'domain': dns, 'convention': 'firstlast' + element_3})
                                elif len(element_1) == 1 and element_1.lower() == first_name[0] and element_2 == last_name:
                                    if element_3 == '':
                                        if not any(c['convention'] == 'flast' and c['domain'] == dns  for c in company_conventions): #vérifier que la convention n'existe pas déjà
                                            company_conventions.append({'domain': dns, 'convention': 'flast'})
                                    else:
                                        if not any(c['convention'] == 'flast' + element_3 and c['domain'] == dns  for c in company_conventions): #vérifier que la convention n'existe pas déjà
                                            company_conventions.append({'domain': dns, 'convention': 'flast' + element_3})
        conventions[company] = company_conventions  # Ajouter les conventions pour cette entreprise dans le dictionnaire global
    return conventions
