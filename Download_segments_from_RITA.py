#-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Berder / load_files_from_USA
# Purpose:     Load data from files from USA's DoT
#
# Author:      Laurent Berder
#
# Created:     22/01/2017
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
from selenium import webdriver
import os
import zipfile

tmp_dir = '/tmp/USA'
full_url = 'http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=293'   # Download file by selecting all fields

if not os.path.isdir(tmp_dir):
    os.mkdir(tmp_dir)


def download_one(month, year):
   """
   Download a single year_month's flights. It is not easy to identify where the files are located, so this function
   mimics a user filling the form, selecting year and month as well as all variables, and clicking download.
   :param month: integer
   :param year: integer
   :return: a single renamed csv file
   """
   end_name = "US_Segments_%s-%s.csv" % (month, year)

   # Set chrome options and reach the website
   options = webdriver.ChromeOptions()
   options.add_experimental_option("prefs", {
       "download.default_directory": tmp_dir,
       "download.prompt_for_download": False,
   })
   driver = webdriver.Chrome(chrome_options=options)
   driver.implicitly_wait(10)
   driver.get(full_url)
   assert "RITA" in driver.title

   # Select the demanded year and month
   driver.find_element_by_xpath("//select[@id='XYEAR']/option[@value=%s]" % year).click()
   driver.find_element_by_xpath("//select[@id='FREQUENCY']/option[@value=%s]" % month).click()

   # Click the "select all variables checkbox", then click download
   """
   It could be useful to select only the required variables instead of all of them, but
   the difference in filesize isn't very important, so I just identified the one button.
   """
   driver.find_element_by_name('AllVars').click()
   driver.find_element_by_name("Download").click()

   # Wait for file to be downloaded
   time.sleep(20)
   driver.close()

   # Identify downloaded zip file, then unzip its content and delete zip file
   zip_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
   zip_ref = zipfile.ZipFile(zip_name, 'r')
   zip_ref.extractall(tmp_dir)
   zip_ref.close()
   os.remove(zip_name)

   # Identify csv file name, and rename to "US_Segment_month-year.csv"
   csv_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
   os.rename(os.path.join(tmp_dir, csv_name), os.path.join(tmp_dir, end_name))
   print("%s downloaded", end_name)

   return end_name


def robot_download(month, year):
   """
   Depending on whether month and/or year are single or multiple values, iterate to download the relevant files
   :param month: integer or tuple of integers
   :param year: integer or tuple of integers
   :return: list of downloaded csv files
   """
   csv_files = []
   if not isinstance(month, tuple) and not isinstance(year, tuple):
       csv_files.append(download_one(month, year))
   else:
       if isinstance(month, tuple) and isinstance(year, tuple):
           for y in year:
               for m in month:
                   csv_files.append(download_one(m, y))
       else:
           if isinstance(month, tuple):
               for m in month:
                   csv_files.append(download_one(m, year))
           else:
               for y in year:
                   csv_files.append(download_one(month, y))
   return csv_files


def main():
   print('Starting to get data from Bureau of Transportation Statistics')
   year = str(input("Enter year(s) to download (separated by a comma"))
   if ',' in year:
       year = year.split(',')
   month = input("Enter month number(s) (separated by a comma")
   if ',' in month:
       month = month.split(',')
   csv_files = robot_download(month, year)


if __name__ == '__main__':
   main()
