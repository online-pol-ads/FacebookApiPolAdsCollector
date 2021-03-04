#!/usr/bin/env python

import argparse
from collections import namedtuple
import configparser
from datetime import datetime
from glob import glob
import json
import math
import os
import psycopg2
import psycopg2.extras
import re
from urllib.parse import urlparse, parse_qs

# ==================== START: COPIED FROM generic_fb_collector.py ====================
AdRecord = namedtuple('AdRecord', ['archive_id',
                                   'page_id',
                                   'image_url',
                                   'text',
                                   'sponsor_label',
                                   'creation_date',
                                   'start_date',
                                   'end_date',
                                   'is_active',
                                   'min_impressions',
                                   'max_impressions',
                                   'min_spend',
                                   'max_spend',
                                   'currency',
                                   'ad_creative_link_caption',
                                   'ad_creative_link_description',
                                   'ad_creative_link_title'])
PageRecord = namedtuple('PageRecord', ['id', 'name'])
SnapshotRegionRecord = namedtuple('SnapshotRegionRecord', ['archive_id', 'name', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
SnapshotDemoRecord = namedtuple('SnapshotDemoRecord', ['archive_id', 'age_range', 'gender', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])

def existing_ads(cursor):
	existing_ad_query = "select archive_id, is_active from ads"
	cursor.execute(existing_ad_query)
	ad_ids = set()
	active_ads = set()
	for row in cursor:
		ad_ids.add(row["archive_id"])
		if row["is_active"] == True:
			active_ads.add(row["archive_id"])

	return (ad_ids, active_ads)

def existing_demos(cursor):
	existing_demo_group_query = "select gender, age, id from demo_groups;"
	cursor.execute(existing_demo_group_query)
	existing_demo_groups = {}
	for row in cursor:
		existing_demo_groups[row["age"]+row["gender"]] = row["id"]

	return existing_demo_groups

def existing_region(cursor):
	existing_regions_query = "select name, id from regions;"
	cursor.execute(existing_regions_query)
	existing_regions = {}
	for row in cursor:
		existing_regions[row["name"]] = row["id"]

	return existing_regions

def existing_page(cursor):
	existing_pages_query = "select page_id, page_name from pages;"
	cursor.execute(existing_pages_query)
	existing_pages = set()
	for row in cursor:
		existing_pages.add(row["page_id"])

	return existing_pages

def existing_sponsors(cursor):
	existing_ad_sponsor_query = "select id, name from ad_sponsors;"
	cursor.execute(existing_ad_sponsor_query)
	existing_ad_sponsors = {}
	for row in cursor:
		existing_ad_sponsors[row["name"]] = row["id"]

	return existing_ad_sponsors

def insert_ad_sponsors(cursor, new_ad_sponsors):
    insert_ad_sponsor = "INSERT INTO ad_sponsors(name) VALUES "
    ad_sponsor_count = 0
    for ad_sponsor in new_ad_sponsors:
        insert_ad_sponsor += cursor.mogrify("(%s),", (ad_sponsor,)).decode('utf-8')
        ad_sponsor_count += 1

        if ad_sponsor_count >= 250:
            insert_ad_sponsor = insert_ad_sponsor[:-1]
            insert_ad_sponsor += ";"
            #print(cursor.mogrify(insert_ad_sponsor))
            cursor.execute(insert_ad_sponsor)
            insert_ad_sponsor = "INSERT INTO ad_sponsors(name) VALUES "
            ad_sponsor_count = 0

    if ad_sponsor_count > 0:
        insert_ad_sponsor = insert_ad_sponsor[:-1]
        insert_ad_sponsor += ";"
        #print(cursor.mogrify(insert_ad_sponsor))
        cursor.execute(insert_ad_sponsor)

def insert_pages(cursor, new_pages):
    insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
    page_count = 0
    for page in new_pages:
        insert_page += cursor.mogrify("(%s, %s),",(page.id, page.name)).decode('utf-8')
        page_count += 1

        if page_count >= 250:
            insert_page = insert_page[:-1]
            insert_page += ";"
            #print(cursor.mogrify(insert_page))
            cursor.execute(insert_page)
            insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
            page_count = 0

    insert_page = insert_page[:-1]
    insert_page += ";"
    if page_count > 0:
        #print(cursor.mogrify(insert_page))
        cursor.execute(insert_page)

def insert_regions(cursor, new_regions):
    insert_region = "INSERT into regions(name) VALUES "
    region_count = 0
    for region in new_regions:
        insert_region += cursor.mogrify("(%s),",(region,)).decode('utf-8')
        region_count += 1

        if region_count >= 250:
            insert_region = insert_region[:-1]
            insert_region += ";"
            cursor.execute(insert_region)
            insert_region = "INSERT INTO regions(name) VALUES "
            region_count = 0

    if region_count > 0:
        insert_region = insert_region[:-1]
        insert_region += ";"
        #print(cursor.mogrify(insert_regions))
        cursor.execute(insert_region)

def insert_demos(cursor, new_demo_groups):
    insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
    demo_group_count = 0
    for key, val in new_demo_groups.items():
        insert_demo_groups += cursor.mogrify("(%s, %s),",(val[0], val[1])).decode('utf-8')
        demo_group_count += 1

        if demo_group_count >= 250:
            insert_demo_groups = insert_demo_groups[:-1]
            insert_demo_groups += ";"
            #print(cursor.mogrify(insert_demo_groups))
            cursor.execute(insert_demo_groups)
            insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
            demo_group_count = 0

    if demo_group_count > 0:
        insert_demo_groups = insert_demo_groups[:-1]
        insert_demo_groups += ";"
        #print(cursor.mogrify(insert_demo_groups))
        cursor.execute(insert_demo_groups)

# ==================== END: COPIED FROM generic_fb_collector.py ====================

class LocalFileSystemCollector:

	def __init__(self, country_code, data_path, data_field, config, verbose = False, insert = False, commit = False):
		self.country_code = country_code
		self.data_path = data_path
		self.data_field = data_field
		self.config = config
		self.verbose = verbose
		self.executeInserts = insert
		self.executeCommits = commit
		self.connection = None
		self.cursor = None
		self.temp = {}

	def run(self):
		self.connectToNYUDatabase()
		self.initNewAds()
		self.selectExistingAds()
		for filename in self.getAllFilenames():
			response_body = self.readResponseBody(filename)
			creation_time = self.readCreationTime(filename)
			creation_date = str(creation_time.date())
			self.parseResponseBody(response_body, creation_date)
		self.previewNewNonAds()
		self.insertNewNonAds()
		self.previewNewAds()
		self.insertNewAds(creation_date)
		self.disconnectFromNYUDatabase()

	def connectToNYUDatabase(self):
		print("Connecting to NYU server...")
		HOST = self.config["POSTGRES"]["HOST"]
		DBNAME = self.config["POSTGRES"]["DBNAME"]
		USER = self.config["POSTGRES"]["USER"]
		PASSWORD = self.config["POSTGRES"]["PASSWORD"]
		PORT = self.config["POSTGRES"]["PORT"]
		DBAuthorize = "host={:s} dbname={:s} user={:s} password={:s} port={:s}".format(HOST, DBNAME, USER, PASSWORD, PORT)
		self.connection = psycopg2.connect(DBAuthorize)
		self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
		print("    Connected.")

	def disconnectFromNYUDatabase(self):
		print("Disconnecting from NYU server...")
		self.connection.close()
		self.cursor = None
		print("    Disconnected.")

	def initNewAds(self):
		print("Initializing new ad data structures...")
		#structures to hold all the new stuff we find
		self.old_ad_count = 0
		self.old_active_ad_count = 0
		self.old_inactive_ad_count = 0
		self.total_ad_count = 0
		self.new_ads = set()
		self.new_ad_sponsors = set()
		self.new_pages = set()
		self.new_demo_groups = {}
		self.new_regions = set()
		self.new_impressions = set()
		self.new_ad_region_impressions = set()
		self.new_ad_demo_impressions = set()
		print("    Initialized.")

	def selectExistingAds(self):
		print("Selecting existing ads, regions, demo groups, pages, and ad sponsors...")

		(self.ad_ids, self.active_ads) = existing_ads(self.cursor)
		print("            Ads = {:7,d} ({:,d} active)".format(len(self.ad_ids), len(self.active_ads)))

		self.existing_regions = existing_region(self.cursor)
		print("        Regions = {:7,d}".format(len(self.existing_regions)))

		self.existing_demo_groups = existing_demos(self.cursor)
		print("    Demo groups = {:7,d}".format(len(self.existing_demo_groups)))

		self.existing_pages = existing_page(self.cursor)
		print("          Pages = {:7,d}".format(len(self.existing_pages)))

		self.existing_ad_sponsors = existing_sponsors(self.cursor)
		print("    Ad sponsors = {:7,d}".format(len(self.existing_ad_sponsors)))

	def getAllFilenames(self):
		filenames = glob("{}/*.json".format(self.data_path))
		filenames = sorted(filenames)
		for filename in filenames:
			yield filename

	def readResponseBody(self, filename):
		print("Reading from {:s}".format(filename))
		with open(filename) as f:
			data = json.load(f)
			if self.data_field in data:
				response_body = data[self.data_field]
				if "data" in response_body:
					print("    Data = {:,d} ads".format(len(response_body["data"])))
				else:
					print("    Data = (none)")
				return response_body
		return None

	def readCreationTime(self, filename):
		timestamp = datetime.fromtimestamp(os.path.getctime(filename))
		print("    Timestamp = {}".format(timestamp))
		return timestamp

	def parseResponseBody(self, results, crawl_date):
		if "data" in results:
			for result in results["data"]:
				self.total_ad_count += 1
				image_url = result["ad_snapshot_url"]
				url_parts = urlparse(image_url)
				archive_id = int(parse_qs(url_parts.query)["id"][0])
				page_id = result["page_id"]
				page_name = result["page_name"] if "page_name" in result else None
				start_date = result["ad_delivery_start_time"]
				currency = result["currency"]
				ad_text = ""
				if "ad_creative_body" in result:
					ad_text = result["ad_creative_body"]
				ad_sponsor_label = ""
				if "funding_entity" in result:
					ad_sponsor_label = result["funding_entity"]

				if ad_sponsor_label not in self.existing_ad_sponsors:
					self.new_ad_sponsors.add(ad_sponsor_label)

				is_active = True
				end_date = None
				if "ad_delivery_stop_time" in result:
					end_date = result["ad_delivery_stop_time"]
					is_active = False

				min_impressions = 0
				max_impressions = 0
				min_spend = 0
				max_spend = 0
				if "impressions" in result:
					min_impressions = result["impressions"]["lower_bound"]
					max_impressions = result["impressions"]["upper_bound"]
				if "spend" in result:
					min_spend = result["spend"]["lower_bound"]
					max_spend = result["spend"]["upper_bound"]

				link_caption = ""
				if "ad_creative_link_caption" in result:
					link_caption = result["ad_creative_link_caption"]
				link_description = ""
				if "ad_creative_link_description" in result:
					link_description = result["ad_creative_link_description"]
				link_title = ""
				if "ad_creative_link_title" in result:
					link_description = result["ad_creative_link_title"]

				if int(page_id) not in self.existing_pages:
					self.new_pages.add(PageRecord(page_id, page_name))


				parsed_end_date = None
				if end_date:
					parsed_end_date = datetime.strptime(end_date[:10], "%Y-%m-%d")
				parsed_start_date = None
				if start_date:
					datetime.strptime(start_date[:10], "%Y-%m-%d")

#				if not is_active and archive_id in self.ad_ids:
#						self.old_inactive_ad_count += 1

				if archive_id in self.ad_ids:
					self.old_ad_count += 1
					if is_active:
						self.old_active_ad_count += 1
					else:
						self.old_inactive_ad_count += 1

				curr_ad = AdRecord(archive_id,
                                   page_id,
                                   image_url,
                                   ad_text,
                                   ad_sponsor_label,
                                   start_date,
                                   start_date,
                                   end_date,
                                   is_active,
                                   min_impressions,
                                   max_impressions,
                                   min_spend,
                                   max_spend,
                                   currency,
                                   link_caption,
                                   link_description,
                                   link_title)


				if is_active or archive_id in self.active_ads or archive_id not in self.ad_ids:
					self.new_impressions.add(curr_ad)
					if "demographic_distribution" not in result:
						print("no demo information in:")
						print(result)
						continue
			
					if "region_distribution" not in result:
						print("no region information in:")
						print(result)
						continue
			
					for demo_result in result["demographic_distribution"]:
						demo_key = demo_result["gender"]+demo_result["age"]
						if demo_key not in self.existing_demo_groups:
							self.new_demo_groups[demo_key] = (demo_result["gender"], demo_result["age"])

						self.new_ad_demo_impressions.add(SnapshotDemoRecord(archive_id,
                                                                            demo_result["age"],
                                                                            demo_result["gender"],
                                                                            float(demo_result["percentage"]) * int(min_impressions),
                                                                            float(demo_result["percentage"]) * int(max_impressions),
                                                                            float(demo_result["percentage"]) * int(min_spend),
                                                                            float(demo_result["percentage"]) * int(max_spend),
                                                                            crawl_date))

					for region_result in result["region_distribution"]:
						if region_result["region"] not in self.existing_regions:
							self.new_regions.add(region_result["region"])
						self.new_ad_region_impressions.add(SnapshotRegionRecord(archive_id,
                                                                                region_result["region"],
                                                                                float(region_result["percentage"]) * int(min_impressions),
                                                                                float(region_result["percentage"]) * int(max_impressions),
                                                                                float(region_result["percentage"]) * int(min_spend),
                                                                                float(region_result["percentage"]) * int(max_spend),
                                                                                crawl_date))

				if archive_id not in self.ad_ids:
					self.new_ads.add(curr_ad)
					self.ad_ids.add(archive_id)


			#we finished parsing each result
			print("Rows processed = {:7,d} ({:,d} old + {:,d} new)".format(self.total_ad_count, self.old_ad_count, self.total_ad_count - self.old_ad_count))
			print("  Old ad count = {:7,d} ({:,d} active + {:,d} inactive)".format(self.old_ad_count, self.old_active_ad_count, self.old_inactive_ad_count))
			print("  New ad count = {:7,d}".format(self.total_ad_count - self.old_ad_count))
			print("  len(new_ads) = {:7,d}".format(len(self.new_ads)))
			print("   len(ad_ids) = {:7,d}".format(len(self.ad_ids)))

	def previewNewNonAds(self):
		print("Prepare to insert new ad sponsors, pages, regions, and demo groups")
		print("    Ad sponsors = {:7,d}".format(len(self.new_ad_sponsors)))
		print("          Pages = {:7,d}".format(len(self.new_pages)))
		print("        Regions = {:7,d}".format(len(self.new_regions)))
		print("    Demo groups = {:7,d}".format(len(self.new_demo_groups)))

	def insertNewNonAds(self):
		if not self.executeInserts:
			print("PREVIEW ONLY")
			print("DO NOT insert new ad sponsors, pages, regions, and demo groups...")
			return

		print("Inserting new ad sponsors, pages, regions, and demo groups...")

		print("    writing {:,d} ad spnosors to db".format(len(self.new_ad_sponsors)))
		insert_ad_sponsors(self.cursor, self.new_ad_sponsors)

		print("    writing {:,d} pages to db".format(len(self.new_pages)))
		insert_pages(self.cursor, self.new_pages)

		print("    writing {:,d} regions to db".format(len(self.new_regions)))
		insert_regions(self.cursor, self.new_regions)

		print("    writing {:,d} demo groups to db".format(len(self.new_demo_groups)))
		insert_demos(self.cursor, self.new_demo_groups)

		if not self.executeCommits:
			print("PREVIEW ONLY")
			print("DO NOT commit to database")
			print("Rolling back all changes...")
			self.connection.rollback()
			print("    Rolled back.")
		else:
			print("Committing to database...")
			self.connection.commit()
			print("    Committed.")

		print("Selecting existing regions, demo groups, pages, and ad sponsors...")

		self.existing_regions = existing_region(self.cursor)
		print("        Regions = {:7,d}".format(len(self.existing_regions)))

		self.existing_demo_groups = existing_demos(self.cursor)
		print("    Demo groups = {:7,d}".format(len(self.existing_demo_groups)))

		self.existing_pages = existing_page(self.cursor)
		print("          Pages = {:7,d}".format(len(self.existing_pages)))

		self.existing_ad_sponsors = existing_sponsors(self.cursor)
		print("    Ad sponsors = {:7,d}".format(len(self.existing_ad_sponsors)))

	def previewNewAds(self):
		print("Prepare to insert new ads and impressions")
		print("            Ads = {:7,d}".format(len(self.new_ads)))
		print("    Impressions = {:7,d} ({:,d} regions, {:,d} demo groups)".format(len(self.new_impressions), len(self.new_ad_region_impressions), len(self.new_ad_demo_impressions)))

	def insertNewAds(self, crawl_date):
		if not self.executeInserts:
			print("PREVIEW ONLY")
			print("DO NOT insert new ads and impressions...")
			return

		print("Inserting new ads and impressions...")

		#write new ads to our database
		print("    writing {:,d} ads to db".format(len(self.new_ads)))
		self._resetPrintSQLStatements(math.ceil(len(self.new_ads) / 250))
		ad_insert_query = "INSERT INTO ads(archive_id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, ad_sponsor_id, is_active, link_caption, link_description, link_title, country_code) VALUES "
		ad_count = 0
		for ad in self.new_ads:
			ad_insert_query += self.cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),", (ad.archive_id, ad.creation_date, ad.start_date, ad.end_date, ad.currency, ad.page_id, ad.image_url, ad.text, self.existing_ad_sponsors[ad.sponsor_label], ad.is_active, ad.ad_creative_link_caption, ad.ad_creative_link_description, ad.ad_creative_link_title, self.country_code)).decode('utf-8')
			ad_count += 1

			if ad_count >= 250:
				ad_insert_query = ad_insert_query[:-1]
				ad_insert_query += ";"
				#print(self.cursor.mogrify(ad_insert_query))
				self._printSQLStatement(ad_insert_query)
				self.cursor.execute(ad_insert_query)
				ad_insert_query = "INSERT INTO ads(archive_id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, ad_sponsor_id, is_active, link_caption, link_description, link_title, country_code) VALUES "
				ad_count = 0

		if ad_count > 0:
			ad_insert_query = ad_insert_query[:-1]
			ad_insert_query += ";"
			#print(self.cursor.mogrify(ad_insert_query))
			self._printSQLStatement(ad_insert_query)
			self.cursor.execute(ad_insert_query)

		impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES "
		impression_count = 0

		print("    writing {:,d} impressions to db".format(len(self.new_impressions)))
		self._resetPrintSQLStatements(math.ceil(len(self.new_impressions) / 250))
		for impression in self.new_impressions:
			impressions_insert_query += self.cursor.mogrify("(%s, %s, %s, %s, %s, %s),", (impression.archive_id, crawl_date, impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend)).decode('utf-8')
			impression_count += 1

			if impression_count >= 250:
				impressions_insert_query = impressions_insert_query[:-1]
				impressions_insert_query += ";"
				#print(self.cursor.mogrify(impressions_insert_query))
				self._printSQLStatement(impressions_insert_query)
				self.cursor.execute(impressions_insert_query)
				impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES "
				impression_count = 0

		if impression_count > 0:
			impressions_insert_query = impressions_insert_query[:-1]
			impressions_insert_query += ";"
			#print(self.cursor.mogrify(impressions_insert_query))
			self._printSQLStatement(impressions_insert_query)
			self.cursor.execute(impressions_insert_query)

		impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
		self._resetPrintSQLStatements(math.ceil(len(self.new_ad_demo_impressions) / 250))
		impression_count = 0
		for impression in self.new_ad_demo_impressions:
			impression_demo_insert_query += self.cursor.mogrify("(%s, %s, %s, %s, %s, %s, current_date),", (impression.archive_id, self.existing_demo_groups[impression.gender + impression.age_range], impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend)).decode('utf-8')
			impression_count += 1

			if impression_count >= 250:
				impression_demo_insert_query = impression_demo_insert_query[:-1]
				impression_demo_insert_query += ";"
				#print(self.cursor.mogrify(impression_demo_insert_query))
				self._printSQLStatement(impression_demo_insert_query)
				self.cursor.execute(impression_demo_insert_query)
				impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
				impression_count = 0

		if impression_count > 0:
			impression_demo_insert_query = impression_demo_insert_query[:-1]
			impression_demo_insert_query += ";"
			#print(self.cursor.mogrify(impression_demo_insert_query))
			self._printSQLStatement(impression_demo_insert_query)
			self.cursor.execute(impression_demo_insert_query)

		impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
		self._resetPrintSQLStatements(math.ceil(len(self.new_ad_region_impressions) / 250))
		impression_count = 0
		for impression in self.new_ad_region_impressions:
			impression_region_insert_query += self.cursor.mogrify("(%s, %s, %s, %s, %s, %s, current_date),", (impression.archive_id, self.existing_regions[impression.name],  impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend)).decode('utf-8')
			impression_count += 1

			if impression_count >= 250:
				impression_region_insert_query = impression_region_insert_query[:-1]
				impression_region_insert_query += ";"
				#print(self.cursor.mogrify(impression_region_insert_query))
				self._printSQLStatement(impression_region_insert_query)
				self.cursor.execute(impression_region_insert_query)
				impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
				impression_count = 0

		if impression_count > 0:
			impression_region_insert_query = impression_region_insert_query[:-1]
			impression_region_insert_query += ";"
			#print(self.cursor.mogrify(impression_region_insert_query))
			self._printSQLStatement(impression_region_insert_query)
			self.cursor.execute(impression_region_insert_query)

		if not self.executeCommits:
			print("PREVIEW ONLY")
			print("DO NOT commit to database")
			print("Rolling back all changes...")
			self.connection.rollback()
			print("    Rolled back.")
		else:
			print("Committing to database...")
			self.connection.commit()
			print("    Committed.")
	
	def _resetPrintSQLStatements(self, maxCount):
		self._printLineCount = 0
		self._printLineMaxCount = maxCount
		
	def _printSQLStatement(self, sql):
		REGEX = r"[\n\r\t]+"
		HEAD_LENGTH = 40
		TAIL_LENGTH = 40
		MAX_LENGTH = HEAD_LENGTH + TAIL_LENGTH
		if self.verbose:
			self._printLineCount += 1
			safe_sql = re.sub(REGEX, " ", sql)
			if len(safe_sql) > MAX_LENGTH:
				print(    "    [{:d}/{:d}] {:s} ... ({:,d} chars) ... {:s}".format(self._printLineCount, self._printLineMaxCount, safe_sql[:HEAD_LENGTH], len(safe_sql) - MAX_LENGTH, safe_sql[-TAIL_LENGTH:]))
			else:
				print(    "    [{:d}/{:d}] {:s}".format(self._printLineCount, self._printLineMaxCount, safe_sql))

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("country", help="Country code used to query the API", type=str)
	parser.add_argument("path", help="A list of JSON files containing API response bodies", type=str)
	parser.add_argument("--field", help="JSON field containging API response body", type=str, default="response_body")
	parser.add_argument("--config", help="Configuration file containing database info", type=str, default="nyu.cfg")
	parser.add_argument("--verbose", help="Print large insertion SQL statements", type=bool, default=False, const=True, nargs="?")
	parser.add_argument("--insert", help="Execute insertion SQL statements", type=bool, default=False, const=True, nargs="?")
	parser.add_argument("--commit", help="Execute commit command", type=bool, default=False, const=True, nargs="?")
	args = parser.parse_args()
	country_code = args.country
	data_path = args.path
	data_field = args.field
	config_filename = args.config
	verbose = args.verbose
	insert = args.insert
	commit = args.commit

	print("country = {:s}".format(country_code))
	print("   path = {:s}".format(data_path))
	print("  field = {:s}".format(data_field))
	print(" config = {:s}".format(config_filename))
	print("verbose = {:s}".format(str(verbose).upper() if verbose else str(verbose).lower()))
	print(" insert = {:s} (option --insert)".format(str(insert).upper() if insert else str(insert).lower()))
	print(" commit = {:s} (option --commit)".format(str(commit).upper() if commit else str(commit).lower()))

	config = configparser.ConfigParser()
	config.read(config_filename)
	collector = LocalFileSystemCollector(
		country_code, data_path, data_field, config,
		verbose = verbose, insert = insert, commit = commit
	)
	collector.run()

if __name__ == "__main__":
	main()
