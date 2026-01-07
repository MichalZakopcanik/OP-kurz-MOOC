# Ziskane data z New York City 311 Service Requests dataset je priamo z linky: 
# https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/explore/query/SELECT%0A%20%20%60unique_key%60%2C%0A%20%20%60created_date%60%2C%0A%20%20%60closed_date%60%2C%0A%20%20%60agency%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60complaint_type%60%2C%0A%20%20%60descriptor%60%2C%0A%20%20%60descriptor_2%60%2C%0A%20%20%60location_type%60%2C%0A%20%20%60incident_zip%60%2C%0A%20%20%60incident_address%60%2C%0A%20%20%60street_name%60%2C%0A%20%20%60cross_street_1%60%2C%0A%20%20%60cross_street_2%60%2C%0A%20%20%60intersection_street_1%60%2C%0A%20%20%60intersection_street_2%60%2C%0A%20%20%60address_type%60%2C%0A%20%20%60city%60%2C%0A%20%20%60landmark%60%2C%0A%20%20%60facility_type%60%2C%0A%20%20%60status%60%2C%0A%20%20%60due_date%60%2C%0A%20%20%60resolution_description%60%2C%0A%20%20%60resolution_action_updated_date%60%2C%0A%20%20%60community_board%60%2C%0A%20%20%60council_district%60%2C%0A%20%20%60police_precinct%60%2C%0A%20%20%60bbl%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60x_coordinate_state_plane%60%2C%0A%20%20%60y_coordinate_state_plane%60%2C%0A%20%20%60open_data_channel_type%60%2C%0A%20%20%60park_facility_name%60%2C%0A%20%20%60park_borough%60%2C%0A%20%20%60vehicle_type%60%2C%0A%20%20%60taxi_company_borough%60%2C%0A%20%20%60taxi_pick_up_location%60%2C%0A%20%20%60bridge_highway_name%60%2C%0A%20%20%60bridge_highway_direction%60%2C%0A%20%20%60road_ramp%60%2C%0A%20%20%60bridge_highway_segment%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60location%60%0AWHERE%0A%20%20%60created_date%60%0A%20%20%20%20BETWEEN%20%222024-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222025-11-01T02%3A45%3A00%22%20%3A%3A%20floating_timestamp%0AORDER%20BY%20%60created_date%60%20DESC%20NULL%20FIRST/page/filter


import pandas as pd
from sodapy import Socrata
import requests
import json
from datetime import datetime
import re
from bs4 import BeautifulSoup
import time, random
import os

mesiace = {
    "január": "01", "február": "02", "marec": "03", "apríl": "04",
    "máj": "05", "jún": "06", "júl": "07", "august": "08",
    "september": "09", "október": "10", "november": "11", "december": "12"
}

menaSuborov = {
    "Staré Mesto": "stareMestoScraper.json",
    "Ružinov": "ruzinovScraper.json",
    "Nové Mesto": "noveMestoScraper.json",
    "Lamač": "lamacScraper.json",
    "Dúbravka": "dubravkaScraper.json",
    "Karlova Ves": "karlovaVesScraper.json",
    "Petržalka": "petrzalkaScraper.json",
    "Rača": "racaScraper.json",
    "Vajnory": "vajnoryScraper.json",
    "Vrakuňa": "vrakunaScraper.json",
    "Podunajské Biskupice": "podunajskeBiskupiceScraper.json",
    "Devínska Nová Ves": "devinskaNovaVesScraper.json",
    "Devín": "devinScraper.json",
    "Záhorská Bystrica": "zahorskaBystricaScraper.json",
    "Rusovce": "rusovceScraper.json",
    "Jarovce": "jarovceScraper.json",
    "Čunovo": "cunovoScraper.json",
}

def date_to_isodate(date_str):
    day,month,year = date_str.replace(".", "").split(" ")
    if len(day) == 1:
        day = "0" + day
    isodate = f"{year}-{mesiace[month]}-{day}"
    return isodate


#oprava jednociferneho dna na format s 0 v subore stareMestoScraper.json, da sa pouzit na lubovolny file
def compare_isodate(isodate_str):
    match = re.fullmatch(r'(\d{4})\-(\d{2})\-(\d{1})',isodate_str)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{int(day):02d}"
    return isodate_str

def change_isodate(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    for item in data:
        print(f"Povodny isodate: {item['isodate']}")
        item['isodate'] = compare_isodate(item['isodate'])
        print(f"Upraveny isodate: {item['isodate']}")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# v linku nasleduju parametre v tomto poradi
cast = "Staré Mesto"
obdobie = "Tento rok"
obec = "Bratislava"
page = "1" #ak je page = 1, v linku sa strana nemusi uviest

#sort nove nie je len od najnovsich po najstarsie podnety, ale napr. novo archivovany podnet spred rokov sa moze stat novym, lebo bol len teraz archivovany
sort = "nove"
tab = "list"

# Mestske caste som ziskal z neoficialnej API OdkazPreStarostu
with open("mestskeCasti.json", "r", encoding="utf-8") as f:
    mestskeCasti = json.load(f)

for mestskaCast in mestskeCasti:
    URL = f"https://novy.odkazprestarostu.sk/dopyty?cast={mestskaCast["name"]}&obec={obec}&sort=nove&tab=list" 
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    pageMax = soup.find("div", class_="pages-con").find_all("a")[-1].get_text(strip=True)
    print(f"Max strana: {pageMax}")
    data = []
    stopBool = False
    filename = ""
    
    for menaSuborovKey in menaSuborov:
        if menaSuborovKey == mestskaCast["name"]:
            filename = menaSuborov[menaSuborovKey]
            break

    if os.path.exists(filename):
        print(f"Subor {filename} uz existuje, idem na dalsiu mestsku cast\n")
        continue
    print(f"Scrapujem mestsku cast: {mestskaCast['name']}, subor: {filename}\n")

    for page in range(1, int(pageMax) + 1):
        print(f"Scrapujem stranu {page} z {pageMax}\n")
        time.sleep(random.uniform(1, 2))
        URL = f"https://novy.odkazprestarostu.sk/dopyty?cast={mestskaCast["name"]}&obec={obec}&page={page}&sort=nove&tab=list" 
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        section = soup.find("section", class_="podnety")
        podnety = section.find_all("a", class_="podnet-block")
        for podnet in podnety:
            time.sleep(random.uniform(0.3, 0.8))
            podnet_id = ""
            podnet_title = ""
            podnet_description = ""
            podnet_date = podnet.find("div", class_="date").get_text(strip=True)
            podnet_isodate = date_to_isodate(podnet_date)
            podnet_stav = podnet.find("div", class_="state").get_text(strip=True)
            podnet_street = ""
            podnet_city_part = mestskaCast["name"]
            podnet_city = obec
            podnet_lat = ""
            podnet_lon = ""
            podnet_title = podnet.find("div", class_="title").get_text(strip=True)

            #Ak je podnet bez nazvu, preskoci sa
            if podnet_title == "Bez názvu":
                continue

            #Ak je podnet spred roku 2024, preskoci sa
            if podnet_isodate < "2024-01-01":
                print("Dostal som sa na rok 2023, koncim.")
                stopBool = True
                break

            #Ziskanie linku na konkretny podnet  
            href = podnet["href"]
            link = f"https://novy.odkazprestarostu.sk{href}"
            print(link)
            match = re.search(r"/dopyty/(\d+)", href)
            if match:
                podnet_id = match.group(1)

            dopyt = requests.get(link)
            dopyt_soup = BeautifulSoup(dopyt.content, "html.parser")
            dopyt_desc = dopyt_soup.find("turbo-frame", id=f"edit_issue_{podnet_id}")
            dopyt_info = dopyt_soup.find("div", class_="podnet-information-con")
            podnet_description = dopyt_desc.find("div", class_="podnet-description").find_all("p")
            podnet_description = ' '.join([p.get_text(strip=True) for p in podnet_description])
            info_items = dopyt_info.find_all("div", class_="information-wrapper")
            count = len(info_items)
            index = 1
            for item in info_items:
                match index:
                    case 5:
                        podnet_categories = info_items[index-1].find_all("a", class_="value")
                        podnet_categories = [category.get_text(strip=True) for category in podnet_categories]
                        index += 1
                    case 6:
                        podnet_competency = info_items[index-1].find("a", class_="value")
                        if podnet_competency is None:
                            podnet_competency = "Iný subjekt" #Iný subjekt je používaný v týchto dátach bežne a dá sa nájsť v <a> (anchor) elemente, našiel sa však aj prípad kedy bola kompetencia Nikto a nebola v <a> elemente, ale v span elemente, a preto padol scraper
                        else:
                            podnet_competency = podnet_competency.get_text(strip=True)    
                        index += 1
                    case 7:
                        podnet_lat = info_items[index-1].find("div", class_="map")["data-map-latitude-value"]
                        podnet_lon = info_items[index-1].find("div", class_="map")["data-map-longitude-value"]
                        podnet_street = info_items[index-1].find("div", class_="value-con").find("a").get_text(strip=True)
                        index += 1
                    case _:
                        index += 1
                        continue

            print(f"ID: {podnet_id}\n Title: {podnet_title}\n Popis: {podnet_description}\n Kat: {podnet_categories[0]}\n Zodpovedný: {podnet_competency}\n Dátum: {podnet_date}\n ISO Dátum: {podnet_isodate}\n Stav: {podnet_stav}\n Ulica: {podnet_street}\n Mestská časť: {podnet_city_part}\n Obec: {podnet_city}\n Lat: {podnet_lat}\n Lng: {podnet_lon}\n")
            data.append({
                "id": podnet_id,
                "title": podnet_title,
                "description": podnet_description,
                "categories": podnet_categories,
                "competency": podnet_competency,
                "date": podnet_date,
                "isodate": podnet_isodate,
                "status": podnet_stav,
                "street": podnet_street,
                "city_part": podnet_city_part,
                "city": podnet_city,
                "latitude": podnet_lat,
                "longitude": podnet_lon
            })
            with open(filename, "w+", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        if stopBool:
            print("Koniec scrapovania")
            break
