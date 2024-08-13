import socket
import smtplib
import dns.resolver
from unidecode import unidecode
import pandas as pd
import re
import signal
import time
from validate_email import validate_email
import threading

class TimeoutError(Exception):
    pass

class Timeout:
    def __init__(self, seconds=6, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)

def syntax_verification(email):
    return validate_email(email)

def dns_verification(domain_name):
    dns.resolver.timeout = 10
    dns.resolver.lifetime = 10
    email_domains = ['.fr', '.com', '.org', '.group', '.eu', '.net',
                     '-ext.fr', '-ext.com', '-ext.org', '-ext.group', '-ext.eu', '-ext.net']
    for domain in email_domains:
        try:
            domain_to_resolve = re.sub(r'[^\w.-]', '', domain_name + domain)
            if not domain_to_resolve:
                continue
            records = dns.resolver.resolve(domain_to_resolve, 'MX')
            Dns_Record = str(records[0].exchange)
            return True, Dns_Record, domain
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers):
            continue
        except dns.resolver.Timeout:
            continue
    return False, None, None

def smtp_verification(Dns_Record, email_address, timeout_seconds=6):
    host = socket.gethostname()
    server = smtplib.SMTP(timeout=timeout_seconds)
    server.set_debuglevel(0)
    start_time = time.time()
    try:
        server.connect(Dns_Record, port=25)
        server.helo(host)
        server.mail('me@domain.com')
        code, message = server.rcpt(email_address)
        server.quit()
        return code == 250
    except smtplib.SMTPConnectError as e:
        return False
    except smtplib.SMTPServerDisconnected as e:
        return False
    except Exception as e:
        return False

def email_verification(email_address, Dns_Record):
    if syntax_verification(email_address):
        if smtp_verification(Dns_Record, email_address):
            return True
    return False

def fill_email_address_chunk(chunk_df):
    for index, row in chunk_df.iterrows():
        if pd.isnull(row['mail_ver']) or str(row['mail_ver']) =='[]' : 
            first_name = unidecode(str(row['First Name']).lower().replace(' ', '-'))
            first_initial = first_name[0]
            last_name = unidecode(str(row['Last Name']).lower().replace(' ', '-'))
            company_name = unidecode(str(row['Company']).lower())
            company_name = re.sub(r'[^a-zA-Z0-9\s]', '', company_name)
            words = company_name.split()
            if len(words) > 2:
                continue
            else:
                company_name = '-'.join(words[:2])
                company_name = company_name.replace(' ', '-')
                if len(words) > 1:
                    for domain in ['', '.', '-']:
                        for i in range(len(words) - 1):
                            for j in range(i + 1, len(words)):
                                combined_name = words[i] + domain + words[j]
                                result = dns_verification(combined_name)
                                if not result[0]:
                                    continue
                                else:
                                    verif, Dns_Record, domain = result
                                    if domain is None:
                                        continue
                                    else:
                                        emails = [
                                            first_name + last_name + '@' + company_name + domain,
                                            first_name + '.' + last_name + '@' + company_name + domain,
                                            first_initial + last_name + '@' + company_name + domain,
                                            first_initial + '.' + last_name + '@' + company_name + domain,
                                        ]
                                        valid_emails = []
                                        for email_address in emails:
                                            if email_verification(email_address, Dns_Record):
                                                valid_emails.append(email_address)
                                        if valid_emails:
                                            chunk_df.at[index, 'mail_ver'] = ';'.join(valid_emails)
                                            break
                else:
                    if not company_name:
                        continue
                    else:
                        result = dns_verification(company_name)
                        if not result[0]:
                            continue
                        else:
                            verif, Dns_Record, domain = result
                            if domain is None:
                                continue
                            else:
                                emails = [
                                    first_name + last_name + '@' + company_name + domain,
                                    first_name + '.' + last_name + '@' + company_name + domain,
                                    first_initial + last_name + '@' + company_name + domain,
                                    first_initial + '.' + last_name + '@' + company_name + domain,
                                ]
                                valid_emails = []
                                for email_address in emails:
                                    if email_verification(email_address, Dns_Record):
                                        valid_emails.append(email_address)
                                if valid_emails:
                                    chunk_df.at[index, 'mail_ver'] = ';'.join(valid_emails)
    return chunk_df

def email_generation_processing(df):
    threads = []
    for i in range(0, len(df), 4):
        chunk_df = df.iloc[i:i+4]
        thread = threading.Thread(target=fill_email_address_chunk, args=(chunk_df,))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return df
