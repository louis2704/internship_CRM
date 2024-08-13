from unidecode import unidecode
import pandas as pd
import logging
def generate_email(row,conventions):
    personal_email_domains = ['icloud', 'hotmail', 'gmail', 'yahoo']
    mails = []
    company = row['Company']
    if isinstance(company, str):  # Vérifier si la valeur est une chaîne de caractères
        company = company.lower()  # Convertir en minuscules
        if company in conventions.keys():
            if conventions[company]:
                email_address = row['Email Address']
                logging.info(f"l'adresse est {email_address}")
                #if pd.isnull(email_address) or email_address.split('@')[1].split('.')[0] in personal_email_domains:
                first_name = unidecode(row['First Name'].lower()).replace(" ", "")
                last_name = unidecode(row['Last Name'].lower()).replace(" ", "")
                for x in conventions[company]:
                    conv = x['convention']
                    domain = x['domain']
                    if conv == 'first.last':
                        mails.append(first_name + '.' + last_name + '@' + domain)
                    elif conv == 'f.last':
                        mails.append(first_name[0] + '.' + last_name + '@' + domain)
                    elif conv == 'flast':
                        mails.append(first_name[0] + last_name + '@' + domain)
                    elif conv == 'flast-ext':
                        mails.append(first_name[0] + last_name + '-ext' + '@' + domain)
    return ';'.join(mails) if mails else None


