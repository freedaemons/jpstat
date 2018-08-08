from bs4 import BeautifulSoup
import requests as requests
import pandas as pandas
import numpy as numpy
import os

import logging
import datetime

from google.cloud import storage
bucket_name = "i-agility-212104.appspot.com"

#Retrieve all months currently available 
col = ['year', 'month', 'url']
top_df = pd.DataFrame(columns=col)
url = "http://www.e-stat.go.jp/SG1/estat/OtherListE.do?bid=000001006005&cycode=1"
path='https://www.e-stat.go.jp'

r = re.get(url)
data = r.text
year='None'
soup = BeautifulSoup(data, "lxml")

for year_section in table.find_all('ul', {'class': 'stat-cycle_ul_other'}):
    header = year_section.find('li', {'class': 'stat-cycle_header'})
    year = header.find('span').get_text(' ', strip=True)

    months_section = year_section.find('li', {'class': 'stat-cycle_item'})
    for month_row in months_section.find_all('div'):
        month_a = month_row.find('a')
        month = month_a.get_text().rstrip('.\n')
        monthurl = path + month_a.get('href')
        
        row = pd.DataFrame([[year, month, monthurl]], columns=col)
        if(len(top_df)==0):
            top_df = row
        else:
            top_df = top_df.append(row, ignore_index=True) #why the hell doesn't df.append work inplace?? Didn't it always use to?

logging.info('DataFrame of months generated: ' + str(top_df.size) + 'months available, from ' 
	+ top_df.loc[len(top_df)-1,'year'] + ' ' + top_df.loc[len(top_df)-1,'month'] + ' to ' 
	+ top_df.loc[0,'year'] + ' ' + top_df.loc[0,'month'])

retrieve_month_excels_starttime = datetime.utcnow()
logging.info('Starting to retrieve urls of every excel sheet at ' + retrieve_month_excels_starttime + ' UTC.')

excels_df = pd.concat([retrieve_month_excels(murl, path) for murl in top_df['url']])
excelref_df = top_df.merge(excels_df, how='left', on='url')

logging.info('Excel URLs retrieved in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')
logging.info(str(len(excelref_df)) + ' files to retrieve.')

#FIXME
# It's neccessary to check the descriptions of the excel files to see if a new description, i.e. new type of excel sheet structure, has been introduced.
# In that scenario we should send a notification or email alert, and write to some log file that a new cleaning method downstream will be required.

exceldesccount_df = excelref_df.groupby(['excel_description']).agg(['count']).sort_values([('year', 'count')], ascending=False).iloc[:,:1]
exceldesccount_df.columns=['count']
exceldesccount_df.reset_index(inplace=True)

# Now actually download the files to local disk, before writing them to Google Cloud Storage (No direct transfer API available yet via python.).
cwd = os.getcwd()
logging.info('Now working in ' + cwd)

mdir = os.path.join('extracts', 'monthly_reports')
if not os.path.exists(mdir):
    os.makedirs(mdir)
logging.info('Saving files to ' + mdir)

# Building a dictionary of month names in a format more suitable for dirnames
months = excelref_df.month.unique() #do not sort, retain the order
monthnum = list(range(1,13,1))
monthdirs = [str(num).zfill(2) + month for num,month in zip(monthnum, months)]
monthnamedict = dict(zip(months, monthdirs))

# Now iterate and download
download_month_excels_starttime = datetime.utcnow()
logging.info('Starting to download excel files for each month at ' + download_month_excels_starttime + ' UTC.')

for index,row in excelref_df.iterrows():
    filedir = os.path.join(mdir, row['year'], monthnamedict.get(row['month']))
    if not os.path.exists(filedir):
        os.makedirs(filedir)
    filename = str(row['excel_num']) + '.xls'
    savepath = os.path.join(filedir, filename)
    if not os.path.isfile(savepath):
        fileresponse = re.get(row['excel_url'], stream=True)
        with open(savepath, 'wb') as out_file:
            out_file.write(fileresponse.content)
        del fileresponse
        print("saved " + savepath)

logging.info('Downloads completed in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')

# Next, upload the files to Google Cloud Storage using the
filepathlist = []
for dirpath, subdir, files in os.walk(mdir):
	for filename in files:
		if filename.endswith('.xls'):
			filepathlist.append(os.path.join(dirpath, filename))

# Upload and change destination path delimiters from windows to unix, if neccessary
upload_month_excels_starttime = datetime.utcnow()
logging.info('Starting to upload excel files for each month to Google Cloud Storage bucket ' + bucket_name + 'at ' + upload_month_excels_starttime + ' UTC.')
[upload_blob(bucket_name, source_file, source_file.replace('\\', '/')) for source_file in filepathlist]
logging.info('Uploads completed in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')


"""
Returns a dataframe of details on excel sheets of data available for a given url
for a particular month
"""
def retrieve_month_excels(murl, path):
    mcols = ['url', 'excel_num', 'excel_description', 'excel_url']
    mdf = pd.DataFrame(columns=mcols)
    mr = re.get(murl)
    mdata = mr.text
    msoup = BeautifulSoup(mdata, "lxml")
    mtable = msoup.find('div', {'class': 'stat-dataset_list-body'})
    for row in mtable.find_all('article', {'class': 'stat-dataset_list-item'}):
        excel_num = row.find('li', {'class': 'stat-dataset_list-detail-item stat-dataset_list-border-top'}).contents[0].replace('\n','')
        excel_description = row.find('a').contents[0]
        excel_url = ''
        excel_a = row.find_all('a')[1]
        
        if(excel_a['data-file_type'] == 'EXCEL'):
            excel_url = path + excel_a['href']
        mdfrow = pd.DataFrame([[murl, excel_num, excel_description, excel_url]], columns=mcols)
        if(len(mdf)==0):
            mdf = mdfrow
        else:
            mdf = mdf.append(mdfrow, ignore_index=True) #why the hell doesn't df.append work inplace?? Didn't it always use to?
    return(mdf)

"""
https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
"""
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))