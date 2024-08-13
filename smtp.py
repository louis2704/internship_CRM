import pandas as pd
import dns.resolver
import smtplib
import socket
from validate_email_address import validate_email
from threading import Thread



def verification_syntaxe(email):
    return validate_email(email)

def verification_smtp(Dns_Record, email_address):
    host = socket.gethostname()
    server = smtplib.SMTP()
    server.set_debuglevel(0)
    try:
        server.connect(Dns_Record, port=25)
        server.helo(host)
        server.mail('me@domain.com')
        code, message = server.rcpt(email_address)
        server.quit()
        return code == 250
    except smtplib.SMTPConnectError:
        return False
    except smtplib.SMTPServerDisconnected:
        return False
    except Exception:
        return False

def verification_dns(email_address):
    domain_name = email_address.split('@')[1]
    try:
        records = dns.resolver.query(domain_name, 'MX')
        mxRecord = records[0].exchange
        mxRecord = str(mxRecord)
        return mxRecord
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.Timeout):
        return None

def verification_email(email):
    enregistrement_dns = verification_dns(email)
    if enregistrement_dns:
        return verification_smtp(enregistrement_dns, email)
    else:
        return False

def verification_mailpd(mail_verifss, mail_global):
    for index, row in mail_verifss.iterrows():
        if not pd.isna(row['mail']):  # Vérifie si la valeur n'est pas NaN
            emails = row['mail'].split(';')  # Séparer les adresses e-mail en utilisant ';' comme délimiteur
            if len(emails) > 1:
                email_verified = ""
                for email in emails:
                    email = str(email)
                    if verification_email(email):
                        email_verified += email + "; "
                mail_verifss.at[index, 'mail_ver'] = email_verified
            else:
                email = row['mail']
                if verification_dns(email):
                    mail_verifss.at[index, 'mail_ver'] = email
    mail_global.update(mail_verifss)
    return mail_verifss, mail_global

def creer_threads(mail_global):
    mail_global['mail_ver'] = ''
    threads = []
    num_threads = 1
    mail_global_len = len(mail_global)
    taille = 20
    if mail_global_len > taille:
        num_threads = mail_global_len // taille + (mail_global_len % taille > 0)

    for i in range(num_threads):
        start_index = i * taille
        end_index = min((i + 1) * taille, mail_global_len)
        mail_slice = mail_global.iloc[start_index:end_index].copy()

        t = Thread(target=verification_mailpd, args=(mail_slice, mail_global))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    return mail_global