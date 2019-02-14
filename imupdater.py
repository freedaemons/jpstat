from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import io
from google.cloud import storage
import logging
import numpy as np
import os
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import requests
import shutil

#localdir = os.path.join('internalmigration-monthly')
#bucket_name = "internalmigration-monthly"
localdir = os.path.join('extracts', 'monthly_reports')
bucket_name = 'i-agility-212104.appspot.com'
conn = psycopg2.connect(user='airflow', password='Xypherium-0',
						dbname='jpstat',	
                        host='35.224.240.50')

# Mapping dictionary for month. monthnamedict['Jun'] > 06Jun
monthnum = list(range(1,13,1))
months = [datetime.strptime(str(month_number), '%m').strftime('%b') for month_number in monthnum]
monthdirs = [str(num).zfill(2) + month for num,month in zip(monthnum, months)]
monthnamedict = dict(zip(months, monthdirs))

def main():
	logging.getLogger().setLevel(logging.INFO)

	logging.info('Starting APScheduler...')
	sched = BlockingScheduler()
	sched.daemonic = False

	@sched.scheduled_job(trigger='cron', hour='0')
	def scrape():
		logging.info('APScheduler cron job starting at ' + str(datetime.datetime.now().isoformat()))
		start = time.time()

		try:
			if has_update():
				update_references()
				update_cloudstore()
				update_db()
		except TimeoutError as te: 
			logging.exception('Connection timed out.')
		except Exception as e: 
			logging.exception('Non-connection related exception ocurred.')

		elapsed = (time.time() - start)
		logging.exception('Runtime: ' + str(datetime.timedelta(seconds=elapsed)))

	sched.start()

def has_update():
	df = get_online_dates()

	df['year'] = pd.to_numeric(df['year'])
	latest_year = df.iloc[df['year'].idxmax()]['year']
	df['month'] = pd.to_numeric([monthnamedict[month][:2] for month in df['month']])
	latest_year_df = df.loc[df['year'] == latest_year]
	latest_month_numeric = latest_year_df.loc[latest_year_df['month'].idxmax()]['month']
	latest_month = datetime.strptime(str(latest_month_numeric), '%m').strftime('%b')

	logging.info('Latest month available: ' + latest_month + ' ' + str(latest_year))

	db_df = get_db_dates()

	db_df['year'] = pd.to_numeric(db_df['year'])
	curr_latest_year = db_df.iloc[db_df['year'].idxmax()]['year']
	db_df['month'] = pd.to_numeric([monthnamedict[month][:2] for month in db_df['month']])
	curr_latest_year_df = db_df.loc[db_df['year'] == latest_year]
	curr_latest_year_numeric = curr_latest_year_df.loc[curr_latest_year_df['month'].idxmax()]['month']
	curr_latest_month = datetime.strptime(str(curr_latest_year_numeric), '%m').strftime('%b')

	logging.info('Latest month scraped: ' + curr_latest_month + ' ' + str(curr_latest_year))

	if latest_year == curr_latest_year and latest_month == curr_latest_month:
		return False

	return True

def update_references():
	new_files_df = get_new_files()

	cur = conn.cursor()
	textstream = io.StringIO()
	new_files_df['excel_description'].replace(['\n', '\t'], '', regex=True, inplace=True)

	new_files_df.to_csv(textstream, sep='\t', header=False, index=False, quoting=csv.QUOTE_NONE)
	textstream.seek(0)
	cur.copy_from(textstream, 'jpstat_excel_urls', null="")
	conn.commit()

def update_cloudstore():
	db_df = get_db_files()

	cloudstore_files = list_xls_blobs(bucket_name)
	logging.info(str(len(cloudstore_files)) + ' files on Google Cloud Storage bucket.')

	download_df = db_df.copy()
	download_df['path'] = os.path.join(localdir.replace('\\', '/'), download_df['year'], download_df['month'].map(monthnamedict), download_df['excel_num'] + '.xls')
	download_df = download_df[~download_df['path'].isin(cloudstore_files)]
	logging.info(str(len(download_df)) + ' files to download.')

	download_excels(download_df, localdir)

	#walk local directory to confirm files to upload
	filepathlist = []
	for dirpath, subdir, files in os.walk(localdir):
		for filename in files:
			if filename.endswith('.xls'):
				filepathlist.append(os.path.join(localdir, dirpath, filename))

	# Upload to GCS from local worker, change destination path delimiters from windows to unix if neccessary
	upload_month_excels_starttime = datetime.utcnow()
	logging.info('Starting to upload excel files for each month to Google Cloud Storage bucket ' + bucket_name + ' at ' + str(upload_month_excels_starttime) + ' UTC.')
	[upload_blob(bucket_name, source_file, source_file.replace('\\', '/')) for source_file in filepathlist]
	logging.info(str(len(filepathlist)) + ' files uploaded in .' + str(datetime.utcnow() - upload_month_excels_starttime) + '.')

#TODO
def update_db():
	return True

def get_new_files():
	top_df = get_online_dates()

	retrieve_month_excels_starttime = datetime.utcnow()
	path = 'https://www.e-stat.go.jp'
	excels_df = pd.concat([retrieve_month_excels_urls(murl, path) for murl in top_df['url']])
	logging.info('Excel URLs retrieved in ' + str(datetime.utcnow() - retrieve_month_excels_starttime) + '.')

	excelref_df = top_df.merge(excels_df, how='left', on='url')
	logging.info(str(len(excelref_df)) + ' files available from the website.')

	db_df = get_db_files()

	disjoint_df = pd.concat([db_df, excelref_df]).drop_duplicates(keep=False, subset=['excel_url']) #new and redacted files
	new_files_df = pd.merge(disjoint_df, excelref_df, how='inner') #new files
	new_files_df = new_files_df[curr_table_df.columns] #reorder columns
	logging.info(str(len(new_files_df)) + ' new files to log.')

	redacted_df = pd.merge(disjoint_df, curr_table_df, how='inner') #redacted files
	logging.info(str(len(redacted_df)) + ' files in database no longer available on website.')

	return new_files_df

def get_online_dates():
	logging.info('Retrieving list of available internal migration data.')

	main_table = []
	url = "http://www.e-stat.go.jp/SG1/estat/OtherListE.do?bid=000001006005&cycode=1"
	path='https://www.e-stat.go.jp'
	r = requests.get(url)
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

	result = pd.DataFrame(main_table)
	logging.info(str(result.size) + ' months of data available online.')
	return result

def get_db_files():
	select_all_sql = "SELECT year, month, url, excel_num, excel_description, excel_url FROM public.jpstat_excel_urls;"
	df = sqlio.read_sql_query(select_all_sql, conn)
	logging.info(str(len(df)) + ' files logged in database.')

	return df

def download_excels(df, path):
	cwd = os.getcwd()
	logging.info('Now working in ' + cwd)
	
	shutil.rmtree(path)
	os.makedirs(path)
	logging.info('Saving files to ' + os.path.join(cwd, localdir))

	download_month_excels_starttime = datetime.utcnow()
	logging.info('Starting to download excel files for each month at ' + str(download_month_excels_starttime) + ' UTC.')

	df.apply(lambda row: download_file(row['excel_url'], row['path']), axis=1)

	logging.info('Downloads completed in ' + str(datetime.utcnow() - download_month_excels_starttime) + '.')

def download_file(source, filepath):
	dirpath = os.path.join(*path.split('/')[:-1])
	if not os.path.exists(dirpath):
		os.makedirs(dirpath)

	response = requests.get(source, stream=True)
	with open(filepath, 'wb') as outfile:
		outfile.write(response.content)
	del response
	logging.info('Saved ' + filepath)

def retrieve_month_excels_urls(murl, path):
    month_table = []
    mr = requests.get(murl)
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

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

if __name__ == "__main__":
	main()