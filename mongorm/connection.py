from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.collection import Collection
connection = None
database = None

connectionSettings = None

stagedIndexes = []
registeredIndexes = []
droppedIndexes = []
collectionIndexInfo = {}

def connect( databaseName, autoEnsure=False, **kwargs ):
	global database, connection, connectionSettings, autoEnsureIndexes

	autoEnsureIndexes = autoEnsure
	
	connectionSettings = {}
	connectionSettings.update( kwargs )
	connectionSettings.update( {
		'database': databaseName
	} )

	# Reset database & connection
	connection = None
	database = None

def getConnection( ):
	global database, connection, connectionSettings
	
	assert connectionSettings is not None, "No database specified: call mongorm.connect() before use"
	
	if connection is None:
		connectionArgs = {}
		
		for key in ['host', 'port', 'replicaSet', 'read_preference']:
			if key in connectionSettings:
				connectionArgs[key] = connectionSettings[key]

		client = MongoReplicaSetClient if 'replicaSet' in connectionArgs else MongoClient
		
		connection = client( **connectionArgs )
	
	return connection

def ensureIndexes( ):
	global stagedIndexes, registeredIndexes, droppedIndexes, collectionIndexInfo

	if not autoEnsureIndexes:
		return

	assert database is not None, "Must be connected to database before ensuring indexes"

	# Ensure indexes on the documents

	for collection, key_or_list, kwargs in stagedIndexes:
		if collection not in database.collection_names():
			Collection(database, collection, create=True)

		indexInfo = collectionIndexInfo.setdefault(collection, database[collection].index_information())

		if isinstance(key_or_list, basestring):
			# if args on the index have changed, drop the index
			key = key_or_list + '_1'
			keyIndexInfo = indexInfo.get(key)
			if keyIndexInfo is None:
				key = key_or_list + '_-1'
				keyIndexInfo = indexInfo.get(key)

			if keyIndexInfo is not None:
				hasChanged = False
				if kwargs.get('unique', False) or keyIndexInfo.get('unique', False):
					if kwargs.get('unique', False) != keyIndexInfo.get('unique', False):
						hasChanged = True
					if kwargs.get('dropDups', False) != keyIndexInfo.get('dropDups', False):
						hasChanged = True

				# TODO: FIX ME
				#if hasChanged:
				#	database[collection].drop_index(key)
				#	droppedIndexes.append(key)

		# Build the index in the background
		kwargs['background'] = True

		database[collection].ensure_index(key_or_list, **kwargs)
	
	registeredIndexes += stagedIndexes
	stagedIndexes = []

def getDatabase( ):
	global database, connectionSettings
	
	if database is None:
		connection = getConnection( )
		databaseName = connectionSettings['database']
		database = connection[databaseName]
		
		if 'username' in connectionSettings and \
			'password' in connectionSettings:
			database.authenticate( connectionSettings['username'], connectionSettings['password'] )

		ensureIndexes()
	
	return database
