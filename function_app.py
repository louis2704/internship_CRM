from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import azure.functions as func
import logging
import json
import time
import io
from segmentation import segmentation_processing
from email_address_generation import email_generation_processing
from linkedin_scrap import linkedin_scrapping
from fonctions_preprocessing import extract_company_name,preprocesstable, extract_company_name
from convention import convention_
from generate_mails import generate_email
from smtp import creer_threads
from traitement import traitement_prescrapping, traitement_postscrapping
from duplicates import duplicates_linkedin,duplicates_outlook,compare_dataframes,compare_dataframes_outlook

app = func.FunctionApp()
# ----------------------------------Traitement d'un nouveau fichier outlook + génération des nouvelles conventions -----------------------------

@app.blob_trigger(arg_name="myblob0", path="segmentation/InputOutlook/{name}",connection="projetlinkedin_STORAGE")
def blob_trigger_convention(myblob0: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob0.name}"
                f"Blob Size: {myblob0.length} bytes")
    connection="DefaultEndpointsProtocol=https;AccountName=projetlinkedin;AccountKey=AccountKey"
    blob_service_client = BlobServiceClient.from_connection_string(connection)
    container_client = ContainerClient.from_connection_string(connection, container_name="segmentation")
    myblob_name=str(myblob0.name)
    myblob_name=myblob_name.split('segmentation/')[1]
    blob_client_csv = blob_service_client.get_blob_client(container="segmentation", blob="Outputprescrap/prescrapping.csv")
    blob_client_total = blob_service_client.get_blob_client(container="segmentation", blob="convention/convention.json")
    

    blob_client = BlobClient.from_connection_string(connection, container_name="segmentation", blob_name=myblob_name)
    csv_stream = blob_client.download_blob()
    df = pd.read_csv(csv_stream, sep=',', encoding_errors='ignore', on_bad_lines='skip')
    processed_data1=preprocesstable(df)
    processed_data=pd.DataFrame(processed_data1)
    filtered_df = processed_data[processed_data['Company Names'].astype(str).eq('nan') | processed_data['Company Names'].astype(str).eq('') |processed_data['Company Names'].isna()].copy()
    filtered_df['Company Names'] = filtered_df['Emails'].apply(extract_company_name)
    processed_data.update(filtered_df)
    conventions=convention_(processed_data)
    json_data = json.dumps(conventions)

    if blob_client_total.exists():
        logging.info("file loading exist")
        json_stream = blob_client_total.download_blob()
        content = json_stream.content_as_text()
        convention = json.loads(content)
        nom_fichier_json="convention.json"  
        merged_json=compare_dataframes_outlook(json_data,convention)
        merged_json=json.dumps(merged_json)
        with open(nom_fichier_json, "w") as f:
            f.write(merged_json)
        with open(nom_fichier_json, "rb") as data:
            blob_client_total.upload_blob(data, overwrite=True)

        if blob_client_csv.exists():
            final_csv = blob_client_csv.download_blob()
            df_final = pd.read_csv(final_csv,encoding_errors='ignore', on_bad_lines='skip')
            resultat_final=creer_threads(df_final)
            df_genered_final = email_generation_processing(resultat_final)
            df_genered_final = traitement_prescrapping(df_genered_final)
            file_name_final = "final.csv"
            container_name_final="segmentation/Output"
            blob_client_ = BlobClient.from_connection_string(connection, container_name=container_name_final, blob_name=file_name_final)
            logging.info("upload blob")
            blob_client_.upload_blob(df_genered_final, overwrite=False)
            logging.info(f"File {file_name_final} processed and uploaded successfully.")
        else:
            logging.info("pas trouvé")
    else:
        logging.info("file non disponible")
        print("file non disponible")
        nom_fichier_json="convention.json"

    blob_client = BlobClient.from_connection_string(connection, container_name="segmentation/convention", blob_name=nom_fichier_json)
    with open(nom_fichier_json, "w") as f:
        f.write(json_data)

    with open(nom_fichier_json, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

# ---------------------- Traitement pour un nouveau fichier de connections Linkedin -------------------

@app.blob_trigger(arg_name="myblob", path="segmentation/InputLinkedin/{name}",connection="projetlinkedin_STORAGE")

def blob_trigger_segmentation(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length}")

    connection = "DefaultEndpointsProtocol=https;AccountName=projetlinkedin;AccountKey=AccountKey"
    blob_service_client = BlobServiceClient.from_connection_string(connection)
    container_name = "segmentation"
    blob_name = myblob.name.split('segmentation/')[1]
    myblob_str=str(myblob.name)
    extension = myblob_str.split('.')[-1]
    owner = myblob_str.split('.')[0]
    owner = owner.split('InputLinkedin/')[1]
    owner = owner.split('_')[0]
    owner=owner.replace('-',' ')
    logging.info(f"Blob: {blob_name}")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    if 'csv' in extension:
        csv_1 = blob_client.download_blob()
        df = pd.read_csv(csv_1,encoding_errors='ignore', skiprows=3,on_bad_lines='skip')
    if 'xlsx' in extension:
        excel_stream = blob_client.download_blob().readall()
        df = pd.read_excel(io.BytesIO(excel_stream),skiprows=3, engine='openpyxl')

    container_name_="segmentation/Output"
    blob_client3 = blob_service_client.get_blob_client(container=container_name_, blob="final.csv")
    df_segmented = segmentation_processing(df)

    if blob_client3.exists():
        csv_final = blob_client3.download_blob()
        df_final = pd.read_csv(csv_final, encoding_errors='ignore')
        df_segmented=compare_dataframes(df_segmented,df_final)
    if len(df_segmented)!=0:
        df_segmented['Fav'] = "off"
        df_segmented['Owner'] = owner

    #------------------------------------ Génération des emails à partir conventions -------------------------------------
        
        blob_client_json = BlobClient.from_connection_string(connection, container_name="segmentation/convention", blob_name="convention.json")
        
        if blob_client_json.exists():
            # Download the CSV file from Azure Blob Storage}"
            json_stream = blob_client_json.download_blob()
            # Lire le contenu du blob
            content = json_stream.content_as_text()
            # Charger les données JSON
            convention = json.loads(content)
            df_segmented['mail'] = df_segmented.apply(lambda row: generate_email(row, convention), axis=1)

        # -------------------------------- verification smtp pour les mails géneré par convention ------------------------------------------

            resultat=creer_threads(df_segmented)
            # Convertir le DataFrame en fichier CSV
            csv_data = resultat.to_csv(index=False)
            # Téléverser le fichier CSV vers Azure Blob Storage avec le nom spécifié
            blob_client = BlobClient.from_connection_string(connection, container_name="segmentation/smtp_convention", blob_name="smptpconvention.csv")
            blob_client.upload_blob(csv_data, overwrite=False)
        else:
            print("json convention non disponible!!")

    else:
        print("No new contact to add")

# ------------------------------------ Génération des émails par smtp uniquement pour les entreprises oùon connait pas la convention ----------------------------------

@app.blob_trigger(arg_name="myblob5", path="segmentation/smtp_convention/{name}",connection="projetlinkedin_STORAGE")

def blob_trigger_generation_email(myblob5: func.InputStream):


    logging.info(f"Python blob trigger function processed blob"

                f"Name: {myblob5.name}"

                f"Blob Size: {myblob5.length} bytes")

    connection = "DefaultEndpointsProtocol=https;AccountName=projetlinkedin;AccountKey=AccountKey"
    blob_service_client = BlobServiceClient.from_connection_string(connection)

    container_name = "segmentation"

    blob_name = myblob5.name.split('segmentation/')[1]

    logging.info(f"Blob: {blob_name}")

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    csv_2 = blob_client.download_blob()

    df_2 = pd.read_csv(csv_2, dtype={'Email_address': 'object'}, encoding_errors='ignore')

    df_genered = email_generation_processing(df_2)
    df_genered = traitement_prescrapping(df_genered)
    csv_data = df_genered.to_csv(index=False)

    blob_client = BlobClient.from_connection_string(connection, container_name="segmentation/Outputprescrap", blob_name='prescrapping.csv')

    blob_client.upload_blob(csv_data, overwrite=False)
#------------ stocker output prescrapping --------------
    # container_name_="segmentation/Output"
    # file_name_ = "prescrapping.csv"
    # blob_client3 = blob_service_client.get_blob_client(container=container_name_, blob=file_name_)
    # if blob_client3.exists():
    #     # csv_final = blob_client3.download_blob()
    #     # df_final = pd.read_csv(csv_final, encoding_errors='ignore')
    #     # df_genered = pd.concat([df_final,df_genered], ignore_index=True)
    #     blob_client3.delete_blob()
    # csv_data_ = df_genered.to_csv(index=False)
    # blob_client_ = BlobClient.from_connection_string(connection, container_name=container_name_, blob_name=file_name_)
    # blob_client_.upload_blob(csv_data_, overwrite=False)




    blob_client_smtp = BlobClient.from_connection_string(connection, container_name="segmentation/smtp_convention", blob_name="smptpconvention.csv")
    if blob_client_smtp.exists():
        blob_client_smtp.delete_blob()
        logging.info("File smptpconvention.csv deleted successfully.")
        


    logging.info("File prescrapping.csv processed and uploaded successfully.")




# -------------------------------------------------------------------------------------------------

@app.blob_trigger(arg_name="myblob6", path="segmentation/Outputprescrap/{name}",connection="projetlinkedin_STORAGE")

def blob_triggertestscrap(myblob6: func.InputStream):

    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob6.name}"
                f"Blob Size: {myblob6.length} bytes")
    connection = "DefaultEndpointsProtocol=https;AccountName=projetlinkedin;AccountKey=AccountKey"
    blob_service_client = BlobServiceClient.from_connection_string(connection)

    container_name = "segmentation"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="Outputprescrap/prescrapping.csv")
    blob_client_loading = blob_service_client.get_blob_client(container="segmentation", blob="Outputprescrap/loading.csv")

    csv_3 = blob_client.download_blob()
    df = pd.read_csv(csv_3, encoding_errors='ignore')

    logging.info("file loading exist or not ?")
    if blob_client_loading.exists():
        logging.info("file loading exist")
        csv_scrap = blob_client_loading.download_blob()
        df_scrap = pd.read_csv(csv_scrap, encoding_errors='ignore')
    else:
        logging.info("file non disponible")
        print("file non disponible")

        df_scrap = pd.DataFrame(columns=[''])
    logging.info("linkedin_scrapping Avant")
    df_loc, df_loading_scrap, stop = linkedin_scrapping(df, df_scrap)
    logging.info("linkedin_scrapping Après")

    if blob_client_loading.exists():
        blob_client_loading.delete_blob()

    if(stop):
        logging.info("Stop")
       
        ## Traitement pour l'output global
        container_name_="segmentation/Output"
        file_name_ = "final.csv"

        blob_client2 = blob_service_client.get_blob_client(container="segmentation/Ressource", blob="cities.csv")
        csv2 = blob_client2.download_blob()
        df_cities = pd.read_csv(csv2,encoding_errors='ignore')
        df_loc=traitement_postscrapping(df_loc,df_cities)

        #Loading the file before concatenation to the file for dataverse
        blob_client_dataverse_up = BlobClient.from_connection_string(connection, container_name="segmentation/addrowt", blob_name="output.csv")
        blob_client_dataverse = blob_service_client.get_blob_client(container="segmentation", blob="addrowt/output.csv")
        logging.info("upload blob")
        if blob_client_dataverse.exists():
                logging.info("delete blob output.csv de addrow")
                blob_client_dataverse.delete_blob()
        csv_dataverse = df_loc.to_csv(index=False)
        blob_client_dataverse_up.upload_blob(csv_dataverse, overwrite=False)






        # recuperer le dernier finchier
        blob_client3 = blob_service_client.get_blob_client(container=container_name_, blob=file_name_)
        if blob_client3.exists():
            csv_final = blob_client3.download_blob()
            df_final = pd.read_csv(csv_final, encoding_errors='ignore')
            df_loc = pd.concat([df_final,df_loc], ignore_index=True)
            blob_client3.delete_blob()
        csv_data = df_loc.to_csv(index=False)
        if blob_client.exists():
            blob_client.delete_blob()
        
        if blob_client_loading.exists():
            blob_client_loading.delete_blob()

    else :
        logging.info("Not Stop")
        container_name_="segmentation/Outputprescrap"
        file_name_="loading.csv"
        csv_data = df_loading_scrap.to_csv(index=False)
        logging.info("not Stop after to csv")

    blob_client_ = BlobClient.from_connection_string(connection, container_name=container_name_, blob_name=file_name_)
    logging.info("upload blob")
    blob_client_.upload_blob(csv_data, overwrite=False)

    logging.info(f"File {file_name_} processed and uploaded successfully.")