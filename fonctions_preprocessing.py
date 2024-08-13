import re
import pandas as pd
# Function to extract emails from a given text
def extract_emails(text):
    return re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)

# Function to extract phone numbers from a given text
def extract_phone_numbers(text):
    return re.findall(r'\b(?:\+\d{1,2}\s?)?\(?\d{1,4}\)?[-.\s]?\d{1,5}[-.\s]?\d{1,5}[-.\s]?\d{1,9}\b', text)



def preprocesstable(df):
    processed_data=[]
    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        # Extract emails from relevant columns
        email_list = []
        for column in ['E-mail Address', 'E-mail 2 Address', 'E-mail 3 Address']:
            if not pd.isnull(row[column]):
                email_list.extend(extract_emails(str(row[column])))

        # Extract information from relevant columns
        company_column = str(row['Company'])

        # If the "Company" column is not empty, use its value
        if company_column.strip():
            company_names = company_column

        # Extract phone numbers from relevant columns
        phone_numbers = []
        for column in ['Home Phone', 'Home Phone 2', 'Business Phone', 'Business Phone 2', 'Mobile Phone', 'Car Phone',
                    'Other Phone', 'Primary Phone', 'Pager', 'Business Fax', 'Home Fax', 'Other Fax',
                    'Company Main Phone', 'Callback', 'Radio Phone', 'Telex', 'TTY/TDD Phone']:
            if not pd.isnull(row[column]):
                phone_numbers.extend(extract_phone_numbers(str(row[column])))

        # Create a dictionary for the processed data
        processed_line = {
            'First Name': row['First Name'],
            'Last Name': row['Last Name'],
            'Emails': email_list,
            'Company Names': company_names if company_names else None,
            'Phone Numbers': phone_numbers if phone_numbers else None
        }

        # Append the dictionary to the list
        processed_data.append(processed_line)

    # Create a new DataFrame from the processed data
    processed_df = pd.DataFrame(processed_data)
    return processed_df
# Function to extract company names based on email domains
def extract_company_name(emails):
    # List of personal email domains to exclude
    personal_email_domains = ['icloud', 'hotmail', 'gmail', 'yahoo']
    if isinstance(emails, list):
        # If "Emails" is a list, consider the first email in the list
        email = emails[0] if emails else None
    else:
        # If "Emails" is not a list, consider it as is
        email = emails

    if email:
        domain_match = re.search(r'@([\w.-]+)', email)
        if domain_match:
            domain = domain_match.group(1).lower()
            domain=domain.split('.')[0]
            # Check if the domain is not in the personal email domains list
            if domain not in personal_email_domains:
                return domain
    
        return None