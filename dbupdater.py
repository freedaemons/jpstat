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
import requests as re


bucket_name = "internalmigration-monthly"
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
			if has_update('https://www.reddit.com/r/singapore/.json'):
				update('https://www.reddit.com/r/singapore/comments/.json')
		except TimeoutError as te: 
			logging.exception('Connection timed out.')
		except Exception as e: 
			logging.exception('Non-connection related exception ocurred.')

		elapsed = (time.time() - start)
		logging.exception('Runtime: ' + str(datetime.timedelta(seconds=elapsed)))

	sched.start()

def has_update():
	main_table = []
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

	logging.info(str(top_df.size) + ' months of data available online.')

	temp_df = top_df.copy()

	temp_df['year'] = pd.to_numeric(temp_df['year'])
	latest_year = temp_df.iloc[temp_df['year'].idxmax()]['year']
	temp_df['month'] = pd.to_numeric([monthnamedict[month][:2] for month in temp_df['month']])
	latest_year_df = temp_df.loc[temp_df['year'] == latest_year]
	latest_month_numeric = latest_year_df.loc[latest_year_df['month'].idxmax()]['month']
	latest_month = datetime.strptime(str(latest_month_numeric), '%m').strftime('%b')

	logging.info('Latest month available: ' + latest_month + ' ' + str(latest_year))

	select_all_sql = "SELECT year, month, url, excel_num, excel_description, excel_url FROM public.jpstat_excel_urls;"
	curr_table_df = sqlio.read_sql_query(select_all_sql, conn)
	logging.info(str(len(curr_table_df)) + ' files logged in database.')

	curr_table_df['year'] = pd.to_numeric(curr_table_df['year'])
	curr_latest_year = curr_table_df.iloc[curr_table_df['year'].idxmax()]['year']
	curr_table_df['month'] = pd.to_numeric([monthnamedict[month][:2] for month in curr_table_df['month']])
	curr_latest_year_df = curr_table_df.loc[curr_table_df['year'] == latest_year]
	curr_latest_year_numeric = curr_latest_year_df.loc[curr_latest_year_df['month'].idxmax()]['month']
	curr_latest_month = datetime.strptime(str(curr_latest_year_numeric), '%m').strftime('%b')

	logging.info('Latest month scraped: ' + curr_latest_month + ' ' + str(curr_latest_year))

	if latest_year == curr_latest_year and latest_month == curr_latest_month:
		return True

	return False

def update():

if __name__ == "__main__":
	main()