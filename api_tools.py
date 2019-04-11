from datetime import datetime, timedelta
import json
import logging
logging.getLogger().setLevel(logging.INFO)
import pandas as pd
import requests
from time import sleep


"""
Call Google Maps API to geocode addresses, staying within free calls limit of 25,000 per day.

Example call: 
google_geocode(df['Address'], 'AIzaSyCFWQSaCSvHuAoIJTVFi-wTYYxohYuFDFA')
"""
def google_geocode(address_list, cred):
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    result = []
        
    LIMIT = 25000
    unused_limit = LIMIT
    period = timedelta(hours=24)
    limit_start_time = datetime.now()
    first_pass = True
    est_passes = len(address_list) / LIMIT
    
    for idx, address in enumerate(address_list):
        answer = {
            'address': address,
            'Formatted Address': None,
            'Latitude': None,
            'Longitude': None
        }

        params = {'address': address, 'key': cred}
        response = requests.get(url, params=params)
        if response.status_code == 400:
            result.append(answer)
            logging.info('address not geocodeable at item ' + str(idx) + ': ' + str(address))
            continue
        if response.status_code != 200:
            result.append(answer)
            logging.info('API unavailable at item ' + str(idx) + ': ' + str(address))
            logging.info(response)
            break

        body = json.loads(response.text)
        try:
            first_match = body['results'][0]
            answer['Formatted Address'] = first_match['formatted_address']
            location = first_match['geometry']['location']
            answer['Latitude'] = location['lat']
            answer['Longitude'] = location['lng']
            result.append(answer)
            logging.info('Item ' + str(idx) + ' geocoded.')
        except (IndexError, KeyError) as e:
            result.append(answer)
            logging.info('No location match for item ' + str(idx) + ': ' + str(address))

        
        
        unused_limit -= 1
        if unused_limit < 1:
            elapsed_period = datetime.now() - limit_start_time
            est_time = datetime.now() + timedelta(days=(est_passes))
            remaining_period = period - elapsed_period
            remaining_seconds = remaining_period.total_seconds()
            if remaining_seconds > 0 and first_pass == True and est_passes > 1:
                logging.info('Reached free limit for today in ' + str(elapsed_period // 3600) + ' hours, ' + str((elapsed_period % 3600) // 60) + ' min.')
                logging.info('Estimated completion at ' + str(est_time.isoformat()[:19]) + ' local time.')
#                logging.info('Resuming in ' + str(remaining_seconds // 3600) + ' hours, ' + str((remaining_seconds % 3600) // 60) + ' min.')
                sleep(remaining_seconds)
            unused_limit = 25000
            start_time = datetime.now()
    
    if str(type(address_list)) == "<class 'pandas.core.series.Series'>":
        result_df = pd.DataFrame(result, columns=['address', 'Formatted Address', 'Latitude', 'Longitude'], index=address_list.index)
    else:
        result_df = pd.DataFrame(result, columns=['address', 'Formatted Address', 'Latitude', 'Longitude'])
    logging.info('Completed geocoding ' + str(len(address_list)) + ' addresses.')          
    return result_df

"""
Call OneMap SG API to geocode addresses, staying within limit of 250 per minute.

Example call: 
google_geocode(df['Address'])
"""
def onemap_geocode(address_list):
    url = 'https://developers.onemap.sg/commonapi/search'
    params = {'searchVal': None, 'returnGeom': 'Y', 'getAddrDetails':'Y', 'pageNum':'1'}
    result = []
    
    LIMIT = 250
    unused_limit = LIMIT
    period = timedelta(seconds=60)
    limit_start_time = datetime.now()
    first_pass = True
    est_passes = len(address_list) / LIMIT
    print(est_passes)
    
    for address in address_list:
        answer = {
            'address': address,
            'Formatted Address': None,
            'Latitude': None,
            'Longitude': None
        }

        params['searchVal'] = address
        response = requests.get(url, params=params)
        if response.status_code != 200:
            result.append(answer)

        body = json.loads(response.text)
        try:
            first_match = body['results'][0]
            answer['Formatted Address'] = first_match['ADDRESS']
            answer['Latitude'] = first_match['LATITUDE']
            answer['Longitude'] = first_match['LONGITUDE']
            result.append(answer)
        except (IndexError, KeyError) as e:
            result.append(answer)

        
        
        unused_limit -= 1
        if unused_limit < 1:
            elapsed_period = datetime.now() - limit_start_time
            est_time = datetime.now() + timedelta(seconds=(60 * est_passes))
            remaining_period = period - elapsed_period
            remaining_seconds = remaining_period.total_seconds()
            if remaining_seconds > 0 and first_pass == True and est_passes > 1:
                logging.info('Reached limit for first minute in ' + str(elapsed_period.total_seconds()) + 's.')
                logging.info('Estimated completion at ' + str(est_time.isoformat()[:19]) + ' local time.')
#                logging.info('Resuming in ' + str(remaining_seconds) + 's.')
                first_pass = False
                sleep(remaining_seconds)
            unused_limit = LIMIT
            limit_start_time = datetime.now()
            
    if str(type(address_list)) == "<class 'pandas.core.series.Series'>":
        result_df = pd.DataFrame(result, columns=['address', 'Formatted Address', 'Latitude', 'Longitude'], index=address_list.index)
    else:
        result_df = pd.DataFrame(result, columns=['address', 'Formatted Address', 'Latitude', 'Longitude'])
    logging.info('Completed geocoding ' + str(len(address_list)) + ' addresses.')          
    return result_df