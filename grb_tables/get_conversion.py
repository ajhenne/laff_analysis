import logging
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np
import concurrent.futures

logger = logging.getLogger('get_tables')
logger.setLevel('INFO')
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def scrape_and_update(row):

    logger.info(f"Starting for {int(row.Trig_ID)}")

    try:
        trig_id = int(row.Trig_ID)
    except:
        logger.debug(f"FAILED - Can't make int for {row.Trig_ID}")

    url = f"https://www.swift.ac.uk/xrt_curves/{trig_id}/"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            fluxcurve_div = soup.find('div', {'id': 'fluxCurve'})
            conversion_line = fluxcurve_div.find('p').text.strip()
        except:
            logger.debug(f"FAILED {trig_id} - no value on webpage.")
    elif response.status_code == 404:
        logger.debug(f"FAILED {trig_id} - no lightcurve for this data.")
    else:
        logger.debug(f"FAILED {trig_id} -failed with error {response.status_code}.")
    
    ## Grab the correct values from line.
    pattern = r'(\d+\.\d+) x 10-(\d+)'
    match  = re.search(pattern, conversion_line)
    A, B = float(match.group(1)), int(match.group(2))
    conversion_factor = A * 10 ** -B

    try:
        data.loc[data[data['Trig_ID'] == trig_id].index, 'conversion'] = conversion_factor
    except:
        logger.debug(f"FAILED {trig_id} - something wrong with assigned data.")
    
    if conversion_factor is float and conversion_factor > 0:
        logger.info(f"Success for {trig_id} - {conversion_factor}")
    else:
        logger.warning(f"Failed for {trig_id}")

if __name__ == '__main__':

    data_filepath = 'grb_tables/all_grb.csv'

    data = pd.read_table(data_filepath, sep=',', header=0)
    data['conversion'] = np.nan

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_and_update, data.itertuples(index=False))

    logger.info(f"Finishing!")
    data.to_csv('grb_tables/all_grb.csv', index=False)

