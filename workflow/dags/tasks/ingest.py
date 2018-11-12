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
bucket_name = "i-agility-212104.apspot.com"
job_start_time = datetime.utcnow().isoformat()[:-7]
mdir = os.path.join(job_start_time, 'extracts', 'monthly_reports')

"""
files-review-task - Review and update table of internal migration excel files available from the Japanese government's E-STAT website.

Section 1: Crawl E-STAT website and build Dataframe of urls to all available excel sheets
Section 2: Compare newly crawled Dataframe of urls to past records on PostgreSQL database table, and identify newly available files.
Section 3: Convert new-file-urls DataFrame to stream and upload to PostgreSQL table on Google Cloud SQL
"""
def files_review_task():
	### Section 1 BEGIN ###
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

	### Section 1 END ###

	### Section 2 BEGIN ###

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

	### Section 2 END ###

	### Section 3 BEGIN ###

	cur = conn.cursor()
	excelurl_textstream = io.StringIO()
	upload_df = new_files_df.copy()
	upload_df['excel_description'].replace(['\n', '\t'], '', regex=True, inplace=True)

	upload_df.to_csv(excelurl_textstream, sep='\t', header=False, index=False, quoting=csv.QUOTE_NONE)
	excelurl_textstream.seek(0) 
	cur.copy_from(excelurl_textstream, 'jpstat_excel_urls', null="") # null values become ''
	conn.commit()

	logging.info('Excel URL database table updated with ' + str(len(upload_df)) + ' new files.')

	### Section 3 END ###

"""
files-downupload-task - Compare available files from E-STAT to files on Google Cloud Storage bucket, and fill in those that are not in GCS.
"""
def files_downupload_task():
	# Now actually download the files to local disk, before writing them to Google Cloud Storage (No direct transfer API available yet via python.).
	cwd = os.getcwd()
	logging.info('Now working in ' + cwd)

	if not os.path.exists(mdir):
	    os.makedirs(mdir)
	logging.info('Saving files to ' + os.path.join(cwd, mdir))

	# Updated table of files available from E-STAT
	sql = "SELECT year, month, url, excel_num, excel_description, excel_url FROM public.jpstat_excel_urls;"
	curr_table_df = sqlio.read_sql_query(sql, conn)

	# Building a dictionary of month names in a format more suitable for dirnames
	months = curr_table_df.month.unique() #do not sort, retain the order
	monthnum = list(range(1,13,1))
	monthdirs = [str(num).zfill(2) + month for num,month in zip(monthnum, months)]
	monthnamedict = dict(zip(months, monthdirs))

	# Files currently in GCS bucket
	existing_files_list = list_xls_blobs(bucket_name)
	logging.info(str(len(existing_files_list)) + 'files on Google Cloud Storage bucket.')
	logging.info(str(len(curr_table_df)) + 'files on jpstat_excel_urls Postgres table.')

	download_df = curr_table_df.copy()
	download_df['path'] = mdir + '/' + download_df['year'] + '/' + download_df['month'].map(monthnamedict) + '/' + download_df['excel_num'] + '.xls'
	logging.info(str(len(download_df)) + 'files to download.')

	# Now iterate and download to local worker
	download_month_excels_starttime = datetime.utcnow()
	logging.info('Starting to download excel files for each month at ' + download_month_excels_starttime + ' UTC.')

	for index,row in download_df.iterrows():
		filedir = os.path.join(mdir, row['year'], monthnamedict.get(row['month']))
		if not os.path.exists(filedir):
			os.makedirs(filedir)
		filename = str(row['excel_num']) + '.xls'
		savepath = os.path.join(filedir, filename)
		#no need to check if file already exists
		fileresponse = re.get(row['excel_url'], stream=True)
		with open(savepath, 'wb') as out_file:
			out_file.write(fileresponse.content)
		del fileresponse
		logging.info("saved " + savepath)

	logging.info('Downloads completed in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')


	# Scan local dir to compile list of files to upload
	this_job_dir = os.path.join(job_start_time)
	filepathlist = []
	for dirpath, subdir, files in os.walk(this_job_dir):
		for filename in files:
			if filename.endswith('.xls'):
				filepathlist.append(os.path.join(dirpath, filename))

	# Upload to GCS from local worker, change destination path delimiters from windows to unix if neccessary
	upload_month_excels_starttime = datetime.utcnow()
	logging.info('Starting to upload excel files for each month to Google Cloud Storage bucket ' + bucket_name + ' at ' + upload_month_excels_starttime + ' UTC.')
	[upload_blob(bucket_name, source_file, source_file.replace('\\', '/').lstrip(job_start_time + '/')) for source_file in filepathlist]
	logging.info(str(len(filepathlist)) + ' files uploaded.')
	logging.info('Uploads completed in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')

#TODO: Groupby available files and scan for new data cleaning logic required.
def files_history_task():
	print("files-history-task: To-Do.")
	datenow = str(datetime.now().date())

	#TODO: Parallel task to downupload - look for new file descriptions and send notification if found (to create new cleaning logic).
	# Use the number and shape of dataframe after ingesting with pandas as the guideline, instead of the file description
	conn.close()

"""
Returns a dataframe of details on excel sheets of data available for a given url
for a particular month
"""
def retrieve_month_excels(murl, path):
    print(murl)
    month_table = []
#     mcols = ['url', 'excel_num', 'excel_description', 'excel_url']
#     mdf = pd.DataFrame(columns=mcols)
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
            
        month_table.append({
            'url': murl,
            'excel_num': excel_num,
            'excel_description': excel_description,
            'excel_url': excel_url
        })
#         mdfrow = pd.DataFrame([[murl, excel_num, excel_description, excel_url]], columns=mcols)
#         if(len(mdf)==0):
#             mdf = mdfrow
#         else:
#             mdf = mdf.append(mdfrow, ignore_index=True) #why the hell doesn't df.append work inplace?? Didn't it always use to?
    mdf = pd.DataFrame(month_table)
    return(mdf)


def list_xls_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    filelist = []
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        path = str(blob.name)
        if(path[-1:] != '/'):
            filelist.append(blob.name)
    
    return filelist

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

        