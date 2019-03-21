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
from tabulate import tabulate

#localdir = os.path.join('internalmigration-monthly')
#bucket_name = "internalmigration-monthly"
#conn = psycopg2.connect(user='fried', password='m3sametA!',
#						dbname='jpstat',	
#						host='wakawaka')
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
				uploaded_files_df = update_cloudstore()
				update_data(uploaded_files)
		except TimeoutError as te: 
			logging.exception('Connection timed out.')
		except Exception as e: 
			logging.exception('Non-connection related exception ocurred.')

		elapsed = (time.time() - start)
		logging.info('Runtime: ' + str(datetime.timedelta(seconds=elapsed)))

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

	return download_df

#TODO
def update_data(df):
	new_cloud_files = list(df['path'])
	table_1_list= [x for x in new_cloud_files if x.split('/')[-1] == '1.xls']
	table_2_list= [x for x in new_cloud_files if x.split('/')[-1] == '2.xls']
	table_3_1_list = [x for x in new_cloud_files if '3-1' in x]
	table_3_2_list = [x for x in new_cloud_files if '3-2' in x]

	shutil.rmtree(localdir)
	os.makedirs(localdir)

	[download_blob(bucket_name, cloud_file, cloud_file) for cloud_file in new_cloud_files]

	in_out_intra_migrants_df = in_out_intra_migrants(table_1_list, table_3_1_list, table_3_2_list)
	trajectories_df = trajectories(table_2_list)

	dataframe_to_postgres(in_out_intra_migrants_df, 'in_out_intra_migrants')
	#TODO dataframe_to_postgres(trajectories_df, 'trajectories')

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
	if len(redacted_df) > 0:
		logging.info(str(len(redacted_df)) + ' files in database no longer available on website.')
		logging.info(tabulate(abcdf, headers='keys', tablefmt='psql'))

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
	blob = bucket.blob(destinationnation_blob_name)

	blob.upload_from_filename(source_file_name)

	print('File {} uploaded to {}.'.format(
		source_file_name,
		destination_blob_name))

def download_blob(bucket_name, source_blob_name, destination_file_name):
	"""Downloads a blob from the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(bucket_name)
	blob = bucket.blob(source_blob_name)

	if '/' in destination_file_name:
		dirpath = os.path.join(*destination_file_name.split('/')[:-1])
		if not os.path.exists(dirpath):
			os.makedirs(dirpath)

	blob.download_to_filename(destination_file_name)

	print('Blob {} downloaded to {}.'.format(
		source_blob_name,
		destination_file_name))

def dataframe_to_postgres(df, tablename):
	cur = conn.cursor()
	textstream = io.StringIO()
	upload_df = df.copy()

	upload_df.to_csv(textstream, sep='\t', header=False, index=False, quoting=csv.QUOTE_NONE)
	textstream.seek(0) 
	cur.copy_from(textstream, tablename, null="") # null values become ''
	conn.commit()

def in_out_intra_migrants(table_1_list, table_3_1_list, table_3_2_list)
	logging.info('Compiling and melting inbound, outbound, and intra-prefecture/city migrants...')

	in_migrants_age_df = combine_clean_3xls_tables(table3_1_list)
	out_migrants_age_df = combine_clean_3xls_tables(table3_2_list)
	internal_migrants_df = combine_clean_1xls_tables(table_1_list)

	jp2en_df = internal_migrants_df[['Prefecture', 'Prefecture_English']].drop_duplicates()
	jp2en_df.set_index('Prefecture', inplace=True)
	jp2en_dict = jp2en_df['Prefecture_English'].to_dict()

	keys_3xls = ['Total', '0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75-79', '80-84', '85-89', '90 & above']
	in_melted_df = pd.melt(in_migrants_age_df, 
							id_vars=['Prefecture Num', 'Prefecture', 'Gender', 'Year', 'Month'], 
							value_name='Inmigrants', 
							value_vars=keys_3xls, 
							var_name='Age bucket')
	out_melted_df = pd.melt(out_migrants_age_df, 
							id_vars=['Prefecture Num', 'Prefecture', 'Gender', 'Year', 'Month'], 
							value_name='Outmigrants', 
							value_vars=keys_3xls, 
							var_name='Age bucket')

	combined_melted_df = pd.merge(in_melted_df, out_melted_df, how='left',
							left_on=list(in_melted_df.columns).remove('Inmigrants'), 
							right_on=list(out_melted_df.columns).remove('Outmigrants'))

	keys_1xls = ['Both', 'Male', 'Female']
	internal_melted_df = pd.melt(internal_migrants_df, 
							 id_vars=['Prefecture Num', 'Prefecture', 'Prefecture_English', 'Year', 'Month', 'Age bucket'], 
							 value_name='Inbternal migrants', 
							 value_vars=keys_1xls, 
							 var_name='Gender')
	
	shared_columns = [x for x in list(internal_melted_df.columns) if x in list(combined_melted_df.columns)]
	final_combined_melted_df = pd.merge(combined_melted_df, internal_melted_df, 
										how='left',
										left_on=shared_columns, 
										right_on=shared_columns)
	final_combined_melted_df['Prefecture_English'] = final_combined_melted_df.apply(lambda x: jp2en_dict.get(x['Prefecture']), axis=1)

	logging.info('Done.')
	return final_combined_melted_df

def combine_clean_3xls_tables(table_excel_list):
	#Always take the last sheet: assume that in the absense of separate sheets, the numbers are for Japanese citizens only
	table_df_list = []

	for filepath in table_excel_list:
		filepath_parts = filepath.split('\\') #change to / for linux
		year = filepath_parts[2]
		month = filepath_parts[3][:2]

		sheets = pd.ExcelFile(filepath).sheet_names
		df = pd.read_excel(filepath, sheet_name=len(sheets)-1)

		messy_df = df.copy()
		messy_df.reset_index(inplace=True)
		messy_df.columns = [x.replace('～', '-') for x in messy_df.iloc[13].replace({
																'Prefectures': 'Prefecture Num',
																'総 数 1)': 'Total',
																'0～4歳': '0～4',
																'90歳以上': '90 & above'
																}).fillna('Prefecture')]
		messy_df.iloc[18,10] = messy_df.iloc[18,8]
		messy_df = messy_df.iloc[18:66,8:]
		messy_df.reset_index(drop=True, inplace=True)
		messy_df.dropna(axis=1, inplace=True)
		messy_df.loc[0,'Prefecture Num'] = '00'

		both_df = messy_df.iloc[:,:22]
		male_df = messy_df.iloc[:,22:44]
		female_df = messy_df.iloc[:,44:66]

		both_df['Gender'] = 'Both'
		male_df['Gender'] = 'Male'
		female_df['Gender'] = 'Female'

		this_month_df = pd.concat([both_df, male_df, female_df])
		this_month_df['Year'] = str(year)
		this_month_df['Month'] = str(month)

		if False not in this_month_df.iloc[:,2:22].applymap(np.isreal).values.flatten():
			table_df_list.append(this_month_df)
		else:
			print('Some non-numeric value found in in/out migration counts.')

	combined_df = pd.concat(table_df_list).reset_index(drop=True)
	return combined_df

def combine_clean_1xls_tables(table_excel_list):
    table_df_list = []

    for filepath in table_excel_list:
        filepath_parts = filepath.split('\\')
        year = filepath_parts[2]
        month = filepath_parts[3][:2]

        sheets = pd.ExcelFile(filepath).sheet_names
        df = pd.read_excel(filepath, sheet_name=len(sheets)-1)

        messy_df = df.copy()
        messy_df.reset_index(inplace=True)
        
        cornerstone = '全国'
        cornerstone_col_idx = [messy_df.columns.get_loc(x) for x in messy_df.columns if cornerstone in messy_df[x].values][0]
        cornerstone_col = messy_df.iloc[:,cornerstone_col_idx]
        cornerstone_row_idx = int(cornerstone_col.index[cornerstone_col == cornerstone][0])
        messy_df = messy_df.iloc[:,cornerstone_col_idx:cornerstone_col_idx+6]

        messy_df.columns = ['Prefecture Num', 'Prefecture', 'Prefecture_English', 'Both', 'Male', 'Female']
        messy_df.dropna(axis=0, subset=['Prefecture Num', 'Both', 'Male', 'Female'], how='any', inplace=True)
        
        #using the fact that np.nan != np.nan
        messy_df['Prefecture'] = messy_df.apply(lambda x: x['Prefecture Num'] if x['Prefecture']!=x['Prefecture'] else x['Prefecture'], axis=1)
        messy_df['Prefecture Num'] = messy_df.apply(lambda x: '00' if x['Prefecture Num']=='全国' else x['Prefecture Num'], axis=1)
        messy_df['Prefecture Num'] = messy_df.apply(lambda x: '00 000' if '計' in x['Prefecture Num'] else x['Prefecture Num'], axis=1)
        ku_codes = {
            '東京圏': '100',
            '名古屋圏': '101',
            '大阪圏': '102'
        }
        messy_df['Prefecture Num'] = messy_df.apply(lambda x: ku_codes.get(x['Prefecture Num']) if x['Prefecture Num'] in ku_codes else x['Prefecture Num'], axis=1)
        messy_df['Prefecture_English'] = messy_df.apply(lambda x: x['Prefecture_English'].rstrip(' 3)'), axis=1)
        messy_df['Prefecture_English'] = messy_df.apply(lambda x: x['Prefecture_English'] + ' cities' if 'major' in x['Prefecture_English'] else x['Prefecture_English'], axis=1)
        
        messy_df['Year'] = str(year)
        messy_df['Month'] = str(month)
        messy_df['Age bucket'] = 'Total'

        messy_df = messy_df.reindex(columns=['Prefecture Num', 'Prefecture', 'Prefecture_English', 'Year', 'Month', 'Age bucket', 'Both', 'Male', 'Female'])
        
        if False not in messy_df.iloc[:,7:10].applymap(np.isreal).values.flatten():
            table_df_list.append(messy_df)
        else:
            print('Some non-numeric value found in internal migration counts.')
            print(messy_df.iloc[:,7:10].head(5))

    combined_df = pd.concat(table_df_list).reset_index(drop=True)
    if '大阪圏' in combined_df['Prefecture Num']: print('matched multiple kanji string!')
    return combined_df

#TODO
def trajectories(table_2_list):
	return False

if __name__ == "__main__":
	main()