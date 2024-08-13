import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
from selenium.webdriver import ChromeOptions
from selenium.common.exceptions import ElementClickInterceptedException 
import os    

def linkedin_scrapping(df, df_scrap) :
    stop = False
    options = ChromeOptions()
    prefs = {"download.default_directory" : os.getcwd()}
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--headless=new")   
    print(len(df_scrap))
    if(len(df) > len(df_scrap) + 10) :
        urls = df['URL'].iloc[len(df_scrap):len(df_scrap)+ 10].tolist()  
    else : 
        urls = df['URL'].iloc[len(df_scrap):len(df)].tolist()  
    locations = []    
    for url in urls:
        driver = webdriver.Chrome(options=options)
        driver.Port=8080
        driver.get(url)
        time.sleep(5)
        try:
            location_element = driver.find_element(By.XPATH, "//div[contains(@class, 'profile-info-subheader')]//div[contains(@class, 'not-first-middot')]//span[1]")
            location = location_element.text.strip()
        except NoSuchElementException:
            location = " "
        locations.append(location)
        print(location)
        driver.quit()
    
    df_scrap_chunk = pd.DataFrame({'Localisation': locations})    
    df_scrap = pd.concat([df_scrap, df_scrap_chunk], ignore_index=True)    

    if(len(df) < len(df_scrap) + 10) :
        df = pd.concat([df.iloc[:, :12], df_scrap], axis=1)
        stop=True

    return df, df_scrap, stop