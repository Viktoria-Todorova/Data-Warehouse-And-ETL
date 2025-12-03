import pandas as pd
import logging
import requests

from bs4 import BeautifulSoup


def extract_weather_from_sinoptik(city: str = 'sofia')->pd.DataFrame:
    url ="https://m.sinoptik.bg/sofia-bulgaria-100727011"
    headers = {'User-Agent': ('Mozilla/5.0'
               "AppleWebKit/537.36 (KHTML, like Gecko)"
               "Chrome/81.0.4044.138 Safari/537.36")}

    logging.info(f"Fetching weather data from {url}")

    try:
        response= requests.get(url, headers=headers,timeout=10)
    except Exception as e:
        logging.error(f"Network error fetching weather data from {url}")
        raise

    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        temp_node = soup.find('span',class_ = 'wfCurrentTemp')
        feel_node = soup.find('span',class_ = 'wfCurrentFeelTemp')

        temperature = temp_node.text.strip() if temp_node else None
        feel = feel_node.text.strip() if feel_node else None

        if temperature is None or feel is None:
            raise ValueError("Could not parse weather data from Sinoptik HTMl")


    except Exception as e:
        logging.error(f"Parsing error for Sinoptik weather data from {url}")
        raise


    df =pd.DataFrame(
        {
            "city": [city.capitalize()],
            "temperature": [temperature],
            "feel": [feel],
        }
    )
    logging.info(f"Extracting weather data for: {city} : {df.to_dict('records')}")
    return df