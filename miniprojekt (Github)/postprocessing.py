# Ziskane data z New York City 311 Service Requests dataset je priamo z linky: 
# https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/explore/query/SELECT%0A%20%20%60unique_key%60%2C%0A%20%20%60created_date%60%2C%0A%20%20%60closed_date%60%2C%0A%20%20%60agency%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60complaint_type%60%2C%0A%20%20%60descriptor%60%2C%0A%20%20%60descriptor_2%60%2C%0A%20%20%60location_type%60%2C%0A%20%20%60incident_zip%60%2C%0A%20%20%60incident_address%60%2C%0A%20%20%60street_name%60%2C%0A%20%20%60cross_street_1%60%2C%0A%20%20%60cross_street_2%60%2C%0A%20%20%60intersection_street_1%60%2C%0A%20%20%60intersection_street_2%60%2C%0A%20%20%60address_type%60%2C%0A%20%20%60city%60%2C%0A%20%20%60landmark%60%2C%0A%20%20%60facility_type%60%2C%0A%20%20%60status%60%2C%0A%20%20%60due_date%60%2C%0A%20%20%60resolution_description%60%2C%0A%20%20%60resolution_action_updated_date%60%2C%0A%20%20%60community_board%60%2C%0A%20%20%60council_district%60%2C%0A%20%20%60police_precinct%60%2C%0A%20%20%60bbl%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60x_coordinate_state_plane%60%2C%0A%20%20%60y_coordinate_state_plane%60%2C%0A%20%20%60open_data_channel_type%60%2C%0A%20%20%60park_facility_name%60%2C%0A%20%20%60park_borough%60%2C%0A%20%20%60vehicle_type%60%2C%0A%20%20%60taxi_company_borough%60%2C%0A%20%20%60taxi_pick_up_location%60%2C%0A%20%20%60bridge_highway_name%60%2C%0A%20%20%60bridge_highway_direction%60%2C%0A%20%20%60road_ramp%60%2C%0A%20%20%60bridge_highway_segment%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60location%60%0AWHERE%0A%20%20%60created_date%60%0A%20%20%20%20BETWEEN%20%222024-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222025-11-01T02%3A45%3A00%22%20%3A%3A%20floating_timestamp%0AORDER%20BY%20%60created_date%60%20DESC%20NULL%20FIRST/page/filter

import pandas as pd
import json
import csv
from datetime import datetime
import re
import time, random
import os
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt


mesiace = {
    "01": "Január", "02": "Február", "03": "Marec", "04": "Apríl",
    "05": "Máj", "06": "Jún", "07": "Júl", "08": "August",
    "09": "September", "10": "Október", "11": "November", "12": "December"
}

menaOblasti = {
    "stareMestoScraper.json": "Staré Mesto",
    "ruzinovScraper.json": "Ružinov",
    "noveMestoScraper.json": "Nové Mesto",
    "lamacScraper.json": "Lamač",
    "dubravkaScraper.json": "Dúbravka",
    "karlovaVesScraper.json": "Karlova Ves",
    "petrzalkaScraper.json": "Petržalka",
    "racaScraper.json": "Rača",
    "vajnoryScraper.json": "Vajnory",
    "vrakunaScraper.json": "Vrakuňa",
    "podunajskeBiskupiceScraper.json": "Podunajské Biskupice",
    "devinskaNovaVesScraper.json": "Devínska Nová Ves",
    "devinScraper.json": "Devín",
    "zahorskaBystricaScraper.json": "Záhorská Bystrica",
    "rusovceScraper.json": "Rusovce",
    "jarovceScraper.json": "Jarovce",
    "cunovoScraper.json": "Čunovo",
}



def createMonthCategories():
    return list(mesiace.values())

# lepsie by bolo pracovat s pandas dataframe, pre ukazku necham tento pristup
def getComplaintCounts(filename = "complaintCount.csv"): 
    baSum = 0
    # Mestke casti Bratislavy - pocet staznosti
    for subor,oblast in menaOblasti.items():
        if os.path.exists(subor):
            with open(subor, "r", encoding="utf-8") as f:
                mestskaCast = json.load(f)
                baSum += len(mestskaCast)

                # Ak neexistuje subor, vytvori sa aj s headerom
                if not os.path.exists(filename):
                    with open(filename, "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(["Area", "Complaint Count"])

                if os.path.exists(filename):
                    with open(filename, "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow([oblast, len(mestskaCast)])
    
    # Bratislava pocet staznosti
    if os.path.exists(filename):
        with open(filename, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Bratislava", baSum])

    # NYC 311 pocet staznosti
    nyc_data = "311_Service_Requests_from_2010_to_Present_20251102.csv"
    if os.path.exists(nyc_data):
            with open(nyc_data, newline='') as f:
                requestAmount = sum(1 for line in f) - 1 # Spocitanie riadkov pre NYC 311 data, -1 kvoli headeru
                with open(filename, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["New York City", requestAmount])


def getComplaintsAmountByMonth(nycDf, filenames = ["complaintsByMonth2024.csv", "complaintsByMonthBA2024.csv"]):
    nycDf = nycDf.copy()
    if "2024" in filenames[0]:
        nycDf = nycDf[nycDf["Created Date"].dt.year == 2024]
    elif "2025" in filenames[0]:
        nycDf = nycDf[nycDf["Created Date"].dt.year == 2025]
    nycDf["Month"] = nycDf["Created Date"].dt.month.astype(str).str.zfill(2).map(mesiace)
    nycDf = nycDf.groupby("Month").size().reset_index(name="Complaint Count")


    baList = []
    for oblast in menaOblasti.keys():
        df = pd.read_json(oblast, encoding="utf-8") 
        if "2024" in filenames[1]:
            df = df[(df["isodate"] >= "2024-01-01") & (df["isodate"] < "2025-01-01")]
        elif "2025" in filenames[1]:
            df = df[df["isodate"] >= "2025-01-01"]
        df["Month"] = df["isodate"].str.slice(5,7).map(mesiace) # Isodate: YYYY-MM-DD
        baList.append(df)   

    baDf = pd.concat(baList, ignore_index=True).groupby("Month").size().reset_index(name="Complaint Count")
    
    # Chronologicke zoradenie mesiacov
    month_order = createMonthCategories() 
    nycDf["Month"] = pd.Categorical(nycDf["Month"], categories=month_order, ordered=True)
    nycDf = nycDf.sort_values("Month") # keby som chcel dalej pracovat s datami pridam za sortovanie reset_index(drop=True)
    baDf["Month"] = pd.Categorical(baDf["Month"], categories=month_order, ordered=True)
    baDf = baDf.sort_values("Month")

    nycDf.to_csv(filenames[0], index=False)
    baDf.to_csv(filenames[1], index=False)


def getComplaintCategoriesAmount(nycDf, year):
    if year == 2024:
        nycDf = nycDf[nycDf["Created Date"].dt.year == 2024]
    elif year == 2025:
        nycDf = nycDf[nycDf["Created Date"].dt.year == 2025]

    nycCategories = nycDf["Complaint Type"].value_counts()
    nycCategories.columns = ["Complaint Type", "Count"]
    if year == 2024:
        nycCategories.to_csv("nycComplaintCategoriesCount2024.csv", index=False)
    elif year == 2025:
        nycCategories.to_csv("nycComplaintCategoriesCount2025.csv", index=False)

    baList = []
    for oblast in menaOblasti.keys():
        df = pd.read_json(oblast, encoding="utf-8") 
        baList.append(df)   

    baDf = pd.concat(baList, ignore_index=True)
    if year == 2024:
        baDf = baDf[(baDf["isodate"] >= "2024-01-01") & (baDf["isodate"] < "2025-01-01")]
    elif year == 2025:
        baDf = baDf[baDf["isodate"] >= "2025-01-01"]

    
    baDf[["category", "subcategory", "type"]] = baDf["categories"].apply(unpack_categories)
    baCategories = baDf["category"].value_counts()
    baSubcategories = baDf["subcategory"].value_counts()
    baTypesOfProblem = baDf["type"].value_counts()
    baCategories.columns = ["Category", "Count"]
    baSubcategories.columns = ["Subcategory", "Count"]
    baTypesOfProblem.columns = ["Type of Problem", "Count"]

    if year == 2024:
        baCategories.to_csv("baComplaintCategoriesCount2024.csv", index=False)
        baSubcategories.to_csv("baComplaintSubcategoriesCount2024.csv", index=False)
        baTypesOfProblem.to_csv("baComplaintTypesOfProblemCount2024.csv", index=False)
    elif year == 2025:
        baCategories.to_csv("baComplaintCategoriesCount2025.csv", index=False)
        baSubcategories.to_csv("baComplaintSubcategoriesCount2025.csv", index=False)
        baTypesOfProblem.to_csv("baComplaintTypesOfProblemCount2025.csv", index=False)


def unpack_categories(cat_list):
    main = cat_list[0] if len(cat_list) > 0 else None
    sub = cat_list[1] if len(cat_list) > 1 else None
    detail = cat_list[2] if len(cat_list) > 2 else None
    return pd.Series([main, sub, detail])


usecols = ["Created Date", "City", "Status", "Complaint Type"]
nycDf = pd.read_csv("311_Service_Requests_from_2010_to_Present_20251102.csv", usecols=usecols)
nycDf["Created Date"] = pd.to_datetime(
        nycDf["Created Date"],
        format="%m/%d/%Y %I:%M:%S %p"
    )

#getComplaintCounts("complaintCount.csv")
#getComplaintsAmountByMonth(nycDf, ["complaintsByMonthNYC2024.csv", "complaintsByMonthBA2024.csv"])
#getComplaintsAmountByMonth(nycDf, ["complaintsByMonthNYC2025.csv", "complaintsByMonthBA2025.csv"])
getComplaintCategoriesAmount(nycDf, 2024)
#getComplaintCategoriesAmount(nycDf, 2025)