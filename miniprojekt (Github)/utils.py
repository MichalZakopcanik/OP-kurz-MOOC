import requests
import json



def saveCategories():
    response = requests.get("https://odkazprestarostu.sk/api/categories")
    categories = response.json()
    with open("kategorie.json","w", encoding = "utf-8") as f:
        json.dump(categories, f, ensure_ascii=False, indent=4)

