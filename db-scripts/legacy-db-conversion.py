# TODO:
# . add alt tags to image links
# . image thumbnails? (featured image)



import psycopg2
import psycopg2.extras
import re
import lxml
import lxml.html
import os
import shutil
import ipdb
import glob
import sys

def grabData(cursor,tableName,crossRef):
	cursor.execute('select * from ' + tableName)
	entries = [entry for entry in cursor]
	print 'grabbed', len(entries), ' items from', tableName
	class Item: pass
	items = {}
	for entry in entries:
		item = Item()
		for key,val in entry.iteritems():
			setattr(item,key,val)
		if hasattr(item, 'id'):
			items[item.id] = item
		item.tableName = tableName
	if tableName == 'entries_sections' or tableName == 'alerts_sections':
		return entries
	crossRef[tableName] = items
	return items
	
def convertTitle(title):
	if title is None:
		return title
	titleNew = title.lower()
	titleNew = re.sub(r' ','-',titleNew)
	titleNew = re.sub(r'[^0-9a-zA-Z\-]+', '', titleNew)
	titleNew = re.sub(r'-+','-',titleNew)
	titleNew = titleNew.rstrip('-').lstrip('-')
	return titleNew

def convertFileName(fileName):
	name,ext = os.path.splitext(fileName)
	ext = ext.lower()
	nameNew = name.lower()
	nameNew = re.sub(r'[ _]','-',nameNew)
	nameNew = re.sub(r'[^0-9a-zA-Z\-]+', '', nameNew)
	nameNew = re.sub(r'-+','-',nameNew)
	nameNew = nameNew.rstrip('-').lstrip('-')
	nameNew += ext
	return nameNew

class MediaEntry: pass

def replaceLink(link, crossRef, rootDir, mediaFiles):
	mediaPrefix = rootDir + 'wp-content/uploads/legacy/'
	imagePrefix = mediaPrefix + 'images/'
	documentPrefix = mediaPrefix + 'documents/'

	linkNew = 'LINK_ERROR'
	pattern = ur'''http://wildequity\.org/([^/]+)/(.*)'''
	match = re.search(pattern,link[2])
	if match is None:
		pattern = ur'''/([^/]+)/(.*)'''
		match = re.match(pattern,link[2])
		if match is None:
			return

	isImage = False
	linkType = match.groups()[0]
	linkDest = match.groups()[1].rstrip('/')
	if linkType not in crossRef:
		print 'wtf:link_type_unknown',link[2]
		return
	if linkDest.isdigit():
		newId = int(linkDest)
		if newId in crossRef[linkType]:
			newEntry = crossRef[linkType][newId]
			if linkType == 'images':
				fileNameOrig = os.path.basename(newEntry.filename)
				fileName = convertFileName(fileNameOrig)
				linkNew = imagePrefix + fileName
				linkTitle = newEntry.title
				img = MediaEntry()
				img.fileName = fileName
				img.isImage = True
				mediaFiles.append(img)
				print 'IMAGE1',fileName
			elif linkType == 'documents':
				versions = [version.id for version in crossRef['versions'].values() if version.document_id==newId]
				fileName = crossRef['versions'][versions[0]].out_filename
				linkNew = documentPrefix + fileName
				doc = MediaEntry()
				doc.fileName = fileName
				doc.isImage = False
				mediaFiles.append(doc)
			elif linkType == 'versions':
				linkNew = documentPrefix + newEntry.out_filename
				doc = MediaEntry()
				doc.fileName = newEntry.out_filename
				doc.isImage = False
				mediaFiles.append(doc)
			else:
				#TODO: no subdirectories?
				#linkNew = rootDir + 'news/' + newEntry.out_title_link
				linkNew = rootDir + newEntry.out_title_link
		else:
			print 'wtf:missing_link',link[2]
	elif linkType == 'images':
		fileNameOrig = os.path.basename(linkDest)
		fileName = convertFileName(fileNameOrig)
		img = MediaEntry()
		img.fileName = fileName
		img.isImage = True
		mediaFiles.append(img)
		print 'IMAGE2',fileName
		linkNew = imagePrefix + fileName
		# TODO: thumbnail etc
	elif linkType == 'documents':
		print 'wtf:weird_document', link[2]
	elif linkType == 'versions':
		print 'wtf:weird_version', link[2]
	else:
		print 'wtf:undefined_link_type', link[2]

	link[0].set(link[1],linkNew)


def createCsvRow(item):
	#post_id,post_name,post_type,post_date,post_title,post_content,post_status,post_category,post_tags,post_thumbnail,news_summary
	s = ''
	s += '"' + str(item.out_id) + '",'
	s += '"' + item.out_title_link + '",'
	if item.tableName in ('sections','pages','faqs','locations','species','alerts'):
		s += '"page",'
	else:
		s += '"post",'
	# TODO: no news type
	#s += '"news",'
	s += '"' + str(item.out_date) + '",'
	s += '"' + item.out_title + '",'
	s += '"' + item.out_content.replace('"', r'\"') + '",'
	s += '"publish",'
	s += '"",'
	s += '"' + ','.join(item.out_tags) + '",'
	s += '"' + item.out_thumb + '",'
	s += '"' + item.out_title + '",'
	s += '\n'
	return s
	

def convertAllData(outputCsv, outputMedia, rootDir, origMediaPrefix, dbName='we_import'):

	# connect to db
	#connection = psycopg2.connect("dbname=we_production")
	connection = psycopg2.connect('dbname='+dbName)
	cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

	origDocPrefix = origMediaPrefix + '/docs/0000/'
	origImagePrefix = origMediaPrefix + '/images/0000/'

	# grab table data
	crossRef = {}
	allEntries = grabData(cursor, 'entries', crossRef)
	allPages = grabData(cursor, 'pages', crossRef)
	allImages = grabData(cursor, 'images', crossRef)
	allDocuments = grabData(cursor, 'documents', crossRef)
	allSpecies = grabData(cursor, 'species', crossRef)
	allSections = grabData(cursor, 'sections', crossRef)
	allEvents = grabData(cursor, 'events', crossRef)
	allFaqs = grabData(cursor,'faqs', crossRef)
	allVersions = grabData(cursor,'versions', crossRef)
	allAlerts = grabData(cursor,'alerts', crossRef)
	allLocations = grabData(cursor,'locations', crossRef)
	rawEntriesSections = grabData(cursor, 'entries_sections', crossRef)
	rawAlertsSections = grabData(cursor,'alerts_sections', crossRef)

	# clean up database connection
	cursor.close()
	connection.close()

	# create entry-section lookup table
	entryToSection = {}
	for raw in rawEntriesSections:
		if raw['entry_id'] not in entryToSection:
			entryToSection[raw['entry_id']] = [raw['section_id']]
		else:
			entryToSection[raw['entry_id']].append(raw['section_id'])

	# create alert-section lookup table
	alertToSection = {}
	for raw in rawAlertsSections:
		if raw['alert_id'] not in alertToSection:
			alertToSection[raw['alert_id']] = [raw['section_id']]
		else:
			alertToSection[raw['alert']].append(raw['section_id'])

	# create media dirs
	try:
		shutil.rmtree(outputMedia)
		os.makedirs(outputMedia)
		os.makedirs(outputMedia + '/images')
		os.makedirs(outputMedia + '/documents')
	except:
		pass

	# find and copy latest version of each media file
	versionLookup = {}
	docFileMap = {}
	for version in crossRef['versions'].values():
		docId = version.document_id
		versions = [(v.id,v.updated_at) for v in crossRef['versions'].values() if v.document_id==docId]
		ids,dates = zip(*versions)
		latestId = ids[dates.index(max(dates))]
		fileNameOrig = crossRef['versions'][latestId].filename
		fileName = convertFileName(fileNameOrig)

		fileNameBase = fileName
		postNum = 1
		while fileName in versionLookup:
			postNum += 1
			name,ext = os.path.splitext(fileNameBase)
			fileName = name + '-' + str(postNum) + ext
		version.out_filename = fileName
		fileNameOrig = '%s%04d/%s' % (origDocPrefix, latestId, fileNameOrig)
		fileNameNew = outputMedia + '/documents/' + fileName
		docFileMap[fileName] = fileNameOrig
		# TODO shutil.copy(fileNameOrig, fileNameNew)
		#print 'copied',fileNameOrig,'to',fileNameNew

	# index image files
	imageFileMap = {}
	for f in glob.glob(origImagePrefix + '/*/*'):
		fileName = convertFileName(os.path.basename(f))
		destFile = outputMedia + '/images/' + fileName
		imageFileMap[fileName] = f

	# convert titles and set tags in all tables
	titleLookup = {}
	for name,table in crossRef.iteritems():
		for curId,item in table.iteritems():
			# convert title
			if hasattr(item,'title') and item.title is not None:
				title = item.title
			elif hasattr(item,'name') and item.name is not None:
				title = item.name
			elif hasattr(item,'filename') and item.filename is not None:
				title = item.filename
			elif hasattr(item,'common_name') and item.common_name is not None:
				title = item.common_name
			titleNew = convertTitle(title)
			titleNewBase = titleNew
			postNum = 1
			while titleNew in titleLookup:
				postNum += 1
				titleNew = titleNewBase + '-' + str(postNum)
			item.out_title = title
			item.out_title_link = titleNew
			titleLookup[titleNew] = True
			
			# convert date
			if hasattr(item,'updated_at'):
				item.out_date = item.updated_at

			# set tags
			if name=='entries':
				if curId in entryToSection:
					item.sections = entryToSection[curId]
				else:
					item.sections = []
			elif name=='actions':
				if curId in actionToSection:
					item.sections = actionToSection[curId]
				else:
					item.sections = []
			elif name=='sections':
				item.sections = [curId]
		
	# translate links in html
	mediaFiles = []
	contentTypes = ['entries','pages','sections','locations','species','events','faqs','alerts']
	for curType in contentTypes:
		for curId,entry in crossRef[curType].iteritems():
			
			# get correct html field
			if hasattr(entry,'body_html'):
				htmlOrig = entry.body_html
			elif hasattr(entry,'description_html'):
				htmlOrig = entry.description_html
			if not htmlOrig:
				entry.out_content = ''
				continue

			# iterate over and translate each link
			tree = lxml.html.fromstring(htmlOrig.decode('utf-8'))
			links = tree.iterlinks()
			for link in links:
				linkBefore = link[0].get(link[1])
				replaceLink(link, crossRef, rootDir, mediaFiles)
				linkAfter = link[0].get(link[1])
				print 'TRANSLATED',linkBefore,'TO',linkAfter
				
			# form new html string
			html = lxml.html.tostring(tree)
			if html.endswith('</div>'):
				html = html[0:-6]
			if html.startswith('<div>'):
				html = html[5:]
			entry.out_content = html
			if '\x2019' in htmlOrig and 'path on the seawall' in htmlOrig:
				print '**********'
				print htmlOrig
				print '++++++++++'
				print html
				#sys.exit(-1)

	# find and copy images
	for media in mediaFiles:
		if media.isImage:
			if media.fileName in imageFileMap:
				destFile = outputMedia + '/images/' + media.fileName
				shutil.copy(imageFileMap[media.fileName], destFile)
				print 'copied image', imageFileMap[media.fileName], media.fileName
			else:
				print 'IMGFILE BAD', media.fileName
		else:
			if media.fileName in docFileMap:
				destFile = outputMedia + '/documents/' + media.fileName
				shutil.copy(docFileMap[media.fileName], destFile)
				print 'copied doc', docFileMap[media.fileName], media.fileName
			else:
				print 'DOCFILE BAD', media.fileName

			
	# collect all items
	allItems = []
	for ref in [crossRef[contentType] for contentType in contentTypes]:
		allItems += ref.values()

	# add remaining fields
	curId = 1
	for item in allItems:
		item.out_id = 10000 + curId
		curId += 1
		item.out_tags = []
		if hasattr(item,'sections'):
			item.out_tags = [crossRef['sections'][tag].title for tag in item.sections]
			print 'TAGS',item.out_tags
		item.out_thumb = '' # TODO: thumb

	# output csv
	f = open(outputCsv,'w')
	f.write('post_id,post_name,post_type,post_date,post_title,post_content,post_status,post_category,post_tags,post_thumbnail,news_summary\n')
	for item in allItems:
		f.write(createCsvRow(item))
	f.close()
	print 'ALL DONE, wrote', len(allItems), 'records'
	
	
	

if __name__=='__main__':

	# for my server
	outputCsv = '/mnt/hgfs/host-data/Projects/Wild Equity/out.csv'
	outputMedia = '/var/www/html/wordpress/wp-content/uploads/legacy'
	rootDir = '/wordpress/'

	# for chris's server
	outputCsv = '/mnt/hgfs/host-data/Projects/Wild Equity/legacy_posts.csv'
	outputMedia = '/mnt/hgfs/host-data/Projects/Wild Equity/legacy';
	rootDir = '/wilequidev/'
	
	# for tilden server
	outputCsv = '/mnt/hgfs/host-data/Projects/Wild Equity/legacy_tilden/legacy_posts.csv'
	outputMedia = '/mnt/hgfs/host-data/Projects/Wild Equity/legacy_tilden/legacy'
	rootDir = '/wpclone/'
	origMediaPrefix = '/mnt/hgfs/host-data/Transfer/WildEquityData'
	dbName = 'we_import'

	convertAllData(outputCsv, outputMedia, rootDir, origMediaPrefix, dbName)

