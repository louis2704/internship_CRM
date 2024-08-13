import pandas as pd
import unidecode
import pycountry

def localisation_treatement(df, dfcities):
    dfcities = dfcities.fillna('')
    # Normalisation des accents dans la colonne 'chefLieu' de dfcities
    dfcities['chefLieu'] = dfcities['chefLieu'].apply(lambda x: unidecode.unidecode(str(x).lower()))

    # Obtenir une liste de noms de pays
    countries = [country.name for country in pycountry.countries]
    countries.extend(['coordonnees'])  # Ajouter 'coordonnées' à la liste des mots à exclure
    countries = [unidecode.unidecode(country.lower()) for country in countries]

    # Remplir la colonne 'Ville'
    df['Ville'] = df['Localisation'].str.split().str[0]
    df['Ville'] = df['Ville'].astype(str)
    df['Ville'] = df['Ville'].str.replace(',', '')
    df['Ville'] = df['Ville'].fillna('')  # Remplacer les valeurs NaN par une chaîne vide
    # Normalisation des accents dans la colonne 'Ville' de df
    df['Ville'] = df['Ville'].apply(lambda x: unidecode.unidecode(str(x).lower()))

    for index, row in df.iterrows():
        localisation = unidecode.unidecode(str(row['Localisation'])).lower()
        if localisation == "" or localisation == "nan":
            df.at[index, 'region'] = ''
        elif localisation not in countries:
            if ',' in str(row['Localisation']):
                split_localisation = str(row['Localisation']).split(',')
                if len(split_localisation) > 1:
                    df.at[index, 'region'] = split_localisation[1].strip()
            else:
                ville = unidecode.unidecode(str(row['Ville']).lower())
                if ville != '' and any(dfcities['chefLieu'].str.lower().str.contains(ville)):
                    df.at[index, 'region'] = dfcities.loc[dfcities['chefLieu'].str.lower().str.contains(ville), 'region'].iloc[0]
                else:
                    df.at[index, 'region'] = ''
        elif localisation in countries:
            df.at[index, 'region'] = str(row['Localisation'])
        
    return df

# Exemple d'utilisation
# df = pd.read_csv('path_to_your_file.csv', encoding_errors='ignore')
# dfcities = pd.read_csv('path_to_your_dfcities.csv', encoding_errors='ignore')
# df_filtered = localisation_treatement(df, dfcities)
# print(df_filtered.head())



def mail_treatement(df):
    # Convertir la colonne 'mail_ver' en chaîne de caractères
    df['mail_ver'] = df['mail_ver'].astype(str)
    df['mail_ver']=df['mail_ver'].str.replace('[','').str.replace(']','').str.replace("'","")
    df['mail_ver']=df['mail_ver'].fillna('')
    df['email_exist'] = df['mail_ver'].apply(lambda x: 'Oui' if x != '' else 'Non')
    return df
def traitement_prescrapping(df):
    df=df[['First Name','Last Name', 'URL','Company','Position','Position Group','Fav','Owner','mail_ver']]
    df=mail_treatement(df)
    for index, row in df.iterrows():
        if ';' in str(row['mail_ver']):
            df.at[index, 'mail1'] = str(row['mail_ver']).split(';')[0]
    df  = df[df['mail1'].notna() & (df['mail1'] != '')]
    return df

def traitement_postscrapping(df,dfcities):
    df['Localisation'] = df['Localisation'].str.strip()
    df=df[['First Name','Last Name', 'URL','Company','Position','Position Group','Fav','Owner','mail_ver','mail1','Localisation']]
    #df  = df[df['Localisation'].notna() & (df['Localisation'] != '') & (df['Localisation'] != ' ')]
    df=localisation_treatement(df,dfcities)
    df['region'] = df['region'].str.replace('Ile-de-France', 'Île-de-France', case=False)

    # Convertir la première lettre en majuscule pour les colonnes 'First Name' et 'Last Name'
    df['First Name'] = df['First Name'].str.title()
    df['Last Name'] = df['Last Name'].str.title()
    df = df.drop(columns="Localisation") 
    df = df.replace(',', ';', regex=True)
    return df


