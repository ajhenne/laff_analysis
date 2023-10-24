import logging
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np

logger = logging.getLogger('get_tables')
logger.setLevel('INFO')
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

data_filepath = 'grb_tables/all_grb.csv'

data = pd.read_table(data_filepath, sep=',', header=0)
data['conversion'] = np.nan

for idx, row in data.iterrows():

    try:
        trig_id = int(row.Trig_ID)
    except:
        logger.critical(f"Failed for {row.Trig_ID}")

    logger.info(f"Scraping for {trig_id}...")
    url = f"https://www.swift.ac.uk/xrt_curves/{trig_id}/"

    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        fluxcurve_div = soup.find('div', {'id': 'fluxCurve'})
        conversion_line = fluxcurve_div.find('p').text.strip()
    except Exception as e:
        logger.critical(f"Fail: {e}")
        continue
    
    ## Grab the correct values from line.
    pattern = r'(\d+\.\d+) x 10-(\d+)'
    match  = re.search(pattern, conversion_line)
    A, B = float(match.group(1)), int(match.group(2))
    conversion_factor = A * 10 ** -B

    data.loc[data.index[idx], 'conversion'] = conversion_factor

    logger.info(f"Success.")

logger.info(f"Finishing!")
data.to_csv('final.csv', index=False)

