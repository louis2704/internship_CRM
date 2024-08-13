import pandas as pd
import re
import emoji
def assign_group(position):
    finance_keywords = ['Daf', 'Raf', 'Cfo', 'Key Account', 'Fsi', 'Grands Comptes', 'Gta Run', 'Compte Clef', 'Comptable', 'Finance', 'Finances', 'Business', 'Financier', 'Financière', 'Cro', 'Financial', 'Account Manager', 'Account Executive','Gestion', 'Comptable Général', 'Comptable Client','Fournisseur']
    president_keywords = ['Fondateur','Cheffe', 'Dirigeant', 'Coo', 'Dirigeante', 'Ceo', 'Chief Executive Officer', 'Président','Dg', 'Directeur Général', 'Directeur General', 'Cofounder', 'Cofondateur', 'Créatrice', 'Gérante', 'Cogérant', 'Gérant','President','Vice Président', 'Pdg', 'Vp', 'Présidente', 'Presidente', 'Owner', 'Propriétaire', 'Fondatrice','Chairman', 'Founder', 'Gerant']
    achat_keywords = ['Achat', 'Acheteur','Purchaser', 'Buyer', 'Acheteuse', 'Produit', 'Product', 'B2B', "Ingenieur D'Affaires", "Chargé d'Affaires", "Charge d'Affaires"]
    data_keywords = ['Data', '¨Powerbi', 'Etl', 'Données', 'Donnees', 'Warehouse', 'Bi', 'Qlik', 'Crm', 'Tests De Performance', 'Snowflake', 'Talend', 'Machine Learning', 'Analyst']
    infra_keywords = ['Cto', 'Architect', 'Infrastructure', 'Infrastructures', 'Sap', 'Architecte', 'Devops', 'Ansible', 'Api', 'Cicd', 'Terraform', 'Sysops','Amdin', 'Sys', 'Cloud', 'Cegid', 'Qualiac']
    dev_keywords = ['Dev', 'Developper', 'Digitalization', 'Digitalisation', 'Progiciel', 'Développeur', 'Ui', 'Ux', 'Java', 'Back End', 'Front End', 'Solution Engineer', 'Solutions Engineer', 'Opentext', 'Full Stack', 'Scrum Master', 'Logiciel', 'Logiciels', 'Software']
    dsi_keywords = ['Dsi', 'Etudes', 'Études', 'Cio', 'Chief Information Officer', "Directeur des Systèmes d'Information"]
    securite_keywords = ['Dynatrace', 'Sécurité','Securite', 'Cybersecurite', 'Cybersecurity', 'Cybersécurité', 'Security','Corporate Compliance Officer', 'Cco', 'Itss', 'Dpo', 'Rssi']

    if isinstance(position, str):
          position_lower = position.lower()
          if any(keyword.lower() in position_lower for keyword in president_keywords):
              return 'Autre'
          elif any(keyword.lower() in position_lower for keyword in finance_keywords):
              return 'Finance'
          elif any(keyword.lower() in position_lower for keyword in dev_keywords):
              return 'Développeur'
          elif any(keyword.lower() in position_lower for keyword in achat_keywords):
              return 'Achat'
          elif any(keyword.lower() in position_lower for keyword in data_keywords):
              return 'Data'
          elif any(keyword.lower() in position_lower for keyword in infra_keywords):
              return 'Infrastructure'
          elif any(keyword.lower() in position_lower for keyword in dsi_keywords):
              return 'DSI'
          elif any(keyword.lower() in position_lower for keyword in securite_keywords):
              return 'Sécurité'
    return 'Autre'
def remove_emojis(text):
    return ''.join(c for c in text if not emoji.is_emoji(c))

# Charger le fichier CSV dans un DataFrame

# Appliquer la fonction pour supprimer les emojis à la colonne "First Name"
def segmentation_processing(df):
    df = df[~df['Company'].str.contains('Aerow', case=False, na=False)]
    df['First Name'] = df['First Name'].astype(str).apply(remove_emojis)
    df['Last Name'] = df['Last Name'].astype(str).apply(remove_emojis)
    df['Position Group'] = df['Position'].apply(assign_group)
    df = df[df['Position Group'] != 'Autre']
    return df