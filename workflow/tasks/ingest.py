from bs4 import BeautifulSoup
import requests as re
import pandas as pd
import pandas.io.sql as sqlio
import numpy as np
import os
import io
import csv
from datetime import datetime
import logging
logging.getLogger().setLevel(logging.INFO)

# https://cloud.google.com/sql/docs/postgres/connect-external-app#languages
import psycopg2
conn = psycopg2.connect(user='airflow', password='Xypherium-0',
						dbname='jpstat',	
                        host='35.224.240.50')

from google.cloud import storage
bucket_name = "i-agility-212104.appspot.com"
mdir = os.path.join('extracts', 'monthly_reports')

def files_review_task():
	#Retrieve all months currently available 
	main_table = []
	#url = input("http://www.e-stat.go.jp/SG1/estat/OtherListE.do?bid=000001006005&cycode=1")
	url = "http://www.e-stat.go.jp/SG1/estat/OtherListE.do?bid=000001006005&cycode=1"
	path='https://www.e-stat.go.jp'
	r = re.get(url)
	data = r.text
	year='None'
	soup = BeautifulSoup(data, "lxml")
	table = soup.find('div', {'class': 'stat-cycle_sheet'})
	for year_section in table.find_all('ul', {'class': 'stat-cycle_ul_other'}):
	    header = year_section.find('li', {'class': 'stat-cycle_header'})
	    year = header.find('span').get_text(' ', strip=True)
	    #   print('year changed to ' + year)
	    months_section = year_section.find('li', {'class': 'stat-cycle_item'})
	    for month_row in months_section.find_all('div'):
	        month_a = month_row.find('a')
	        month = month_a.get_text().rstrip('.\n')
	        monthurl = path + month_a.get('href')
	        
	        main_table.append({
	            'year': year,
	            'month': month,
	            'url': monthurl
	        })

	top_df = pd.DataFrame(main_table)

	logging.info('DataFrame of months generated: ' + str(top_df.size) + ' months available, from ' 
		+ top_df.loc[len(top_df)-1,'year'] + ' ' + top_df.loc[len(top_df)-1,'month'] + ' to ' 
		+ top_df.loc[0,'year'] + ' ' + top_df.loc[0,'month'])

	retrieve_month_excels_starttime = datetime.utcnow()
	logging.info('Starting to retrieve urls of every excel sheet at ' + str(retrieve_month_excels_starttime) + ' UTC.')

	excels_df = pd.concat([retrieve_month_excels(murl, path) for murl in top_df['url']])
	excelref_df = top_df.merge(excels_df, how='left', on='url')

	logging.info('Excel URLs retrieved in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')
	logging.info(str(len(excelref_df)) + ' files available from the website.')

	select_all_sql = "SELECT year, month, url, excel_num, excel_description, excel_url FROM public.jpstat_excel_urls;"
	curr_table_df = sqlio.read_sql_query(select_all_sql, conn)
	logging.info(str(len(curr_table_df)) + ' files logged in database.')

	disjoint_df = pd.concat([curr_table_df, excelref_df]).drop_duplicates(keep=False, subset=['excel_url'])
	new_files_df = pd.merge(disjoint_df, excelref_df, how='inner')
	new_files_df = new_files_df[curr_table_df.columns]
	logging.info(str(len(new_files_df)) + ' new files to log.')

	missing_df = pd.merge(disjoint_df, curr_table_df, how='inner')
	logging.info(str(len(missing_df)) + ' files in database no longer available on website.')

	#Convert DataFrame to stream and upload to PostgreSQL table on Google Cloud SQL
	cur = conn.cursor()
	excelurl_textstream = io.StringIO()
	upload_df = new_files_df.copy()
	upload_df['excel_description'].replace(['\n', '\t'], '', regex=True, inplace=True)

	upload_df.to_csv(excelurl_textstream, sep='\t', header=False, index=False, quoting=csv.QUOTE_NONE)
	excelurl_textstream.seek(0) 
	cur.copy_from(excelurl_textstream, 'jpstat_excel_urls', null="") # null values become ''
	conn.commit()

	logging.info('Excel URL database table updated with ' + str(len(new_files_df)) + ' new files.')

	
	exceldesccount_df = excelref_df.groupby(['excel_description']).agg(['count']).sort_values([('year', 'count')], ascending=False).iloc[:,:1]
	exceldesccount_df.columns=['count']
	exceldesccount_df.reset_index(inplace=True)

	exceldesccount_textstream = io.StringIO()
	exceldesccount_df.to_csv(exceldesccount_textstream, sep='\t', header=False, index=False)
	exceldesccount_textstream.seek(0)
	cur.copy_from(exceldesccount_textstream, 'jpstat_excel_descriptions', null="") # null values become ''
	conn.commit()

#FIXME: This task, unlike the review task that builds the url table, needs to be parallelizable
def files_downupload_task():
	# Now actually download the files to local disk, before writing them to Google Cloud Storage (No direct transfer API available yet via python.).
	cwd = os.getcwd()
	logging.info('Now working in ' + cwd)

	if not os.path.exists(mdir):
	    os.makedirs(mdir)
	logging.info('Saving files to ' + os.path.join(cwd, mdir))

	sql = "SELECT year, month, url, excel_num, excel_description, excel_url FROM public.jpstat_excel_urls;"
	curr_table_df = sqlio.read_sql_query(sql, conn)

	# Building a dictionary of month names in a format more suitable for dirnames
	months = curr_table_df.month.unique() #do not sort, retain the order
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
	        logging.info("saved " + savepath)

	logging.info('Downloads completed in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')

#FIXME: This task needs to be performed local to the machine that performed the parallel download task, possibly as a subDAG?
def files_history_task():
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
    logging.info("Retrieved excel URLs from month-URL: " + murl + '.')
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

    logging.info('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))



        