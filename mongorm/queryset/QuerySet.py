from mongorm.queryset.Q import Q
from mongorm.util import sortListToPyMongo
import pymongo

from mongorm.DocumentRegistry import DocumentRegistry

from mongorm.blackMagic import serialiseTypesForDocumentType

PROJECTIONS = frozenset(['slice'])

class QuerySet(object):
	def __init__( self, document, collection, query=None, orderBy=None, fields=None, timeout=True, readPref=None, types=None ):
		self.document = document
		self.documentTypes = serialiseTypesForDocumentType( document )
		self.collection = collection
		self.orderBy = []
		self._fields = fields
		self.timeout = timeout
		self.readPref = readPref
		self.types = []
		if orderBy is not None:
			self.orderBy = orderBy[:]
		self._savedCount = None
		self._savedItems = None
		if query is None:
			self.query = Q( )
		else:
			self.query = query
		if types:
			for subclass in types:
				if not issubclass(subclass, self.document):
					raise TypeError, "'%s' is not a subclass of '%s'" % (subclass, self.document)
				self.types.append( subclass )
	
	def _getNewInstance( self, data ):
		documentName = data.get( '_types', [self.document.__name__] )[0]
		documentClass = DocumentRegistry.getDocument( documentName )
		assert issubclass( documentClass, self.document )
		return documentClass( )._fromMongo( data )

	def _get_kwargs( self ):
		return {
			'query': self.query,
			'orderBy': self.orderBy,
			'fields': self._fields,
			'timeout': self.timeout,
			'readPref': self.readPref,
			'types': self.types,
		}
	
	def get( self, query=None, **search ):
		if query is None:
			query = Q( **search )
		self.query &= query
		#self._mergeSearch( search )
		#print 'get:', newQuery.toMongo( self.document )
		
		# limit of 2 so we know if multiple matched without running a count()
		result = list( self._do_find( limit=2 ) )
		
		if len(result) == 0:
			raise self.document.DoesNotExist( )
		
		if len(result) == 2:
			raise self.document.MultipleObjectsReturned( )
		
		return self._getNewInstance( result[0] )
	
	def all( self ):
		return self

	def close( self ):
		if self._savedItems:
			self._savedItems.close()
	
	def filter( self, query=None, **search ):
		if query is None:
			query = Q( **search )
		kwargs = self._get_kwargs( )
		kwargs['query'] &= query
		return QuerySet( self.document, self.collection, **kwargs )

	def no_timeout( self ):
		kwargs = self._get_kwargs( )
		kwargs['timeout'] = False
		return QuerySet( self.document, self.collection, **kwargs )

	def read_preference( self, readPref ):
		kwargs = self._get_kwargs( )
		kwargs['readPref'] = readPref
		return QuerySet( self.document, self.collection, **kwargs )

	def subtypes( self, *types ):
		kwargs = self._get_kwargs( )
		kwargs['types'] = types
		return QuerySet( self.document, self.collection, **kwargs )
	
	def count( self ):
		if self._savedCount is None:
			if self._savedItems is None:
				self._savedCount = self.collection.find( self._get_query( ) ).count( )
			else:
				self._savedCount = self._savedItems.count( )
		
		return self._savedCount
	
	def __len__( self ):
		return self.count( )
	
	def delete( self ):
		self.collection.remove( self.query.toMongo( self.document ) )
	
	def _prepareActions( self, **actions ):
		updates = {}
		
		for action, value in actions.iteritems( ):
			assert '__' in action, 'Action "%s" not legal for update' % (action,)
			modifier, fieldName = action.split( '__', 1 )
			assert modifier in ['set', 'unset', 'setOnInsert', 'inc', 'dec', 'push', 'pushAll', 'pull', 'pullAll'], 'Unknown modifier "%s"' % modifier
			
			if '$'+modifier not in updates:
				updates['$'+modifier] = {}
			
			translatedName = fieldName.replace('__', '.')
			
			mongoValues = Q( { fieldName: value } ).toMongo( self.document, forUpdate=True, modifier=modifier )
			#print mongoValues
			mongoValue = mongoValues[translatedName]
			
			updates['$'+modifier].update( {
				translatedName: mongoValue
			} )
		
		return updates
	
	def update( self, upsert=False, safeUpdate=False, modifyAndReturn=False, returnAfterUpdate=False, updateAllDocuments=False, **actions ):
		"""Performs an update on the collection, using MongoDB atomic modifiers.
		
		If upsert is specified, the document will be created if it doesn't exist.
		If safeUpdate is specified, the success of the update will be checked and
		the number of modified documents will be returned.
		
		If modifyAndReturn is specified, a findAndModify operation will be executed
		instead of an update operation. The *original* document instance (before any
		modifications) will be returned, unless returnAfterUpdate is True. If no 
		document matched the specified query, None will be returned."""
		
		updates = self._prepareActions( **actions )
		
		# XXX: why was this here? we shouldn't be forcing this
		#if '$set' not in updates:
		#	updates['$set'] = {}
		#
		#updates['$set'].update( self.query.toMongo( self.document, forUpdate=True ) )

		#print 'query:', self.query.toMongo( self.document )
		#print 'update:', updates
		
		query = self._get_query( forUpsert=True )
		#print query, 'query'
		#print updates, 'update'
		
		# {'_types': {$all:['BaseThingUpsert']}, 'name': 'upsert1'}
		# {'$set': {'value': 42}, '$addToSet': {'_types': {$each: ['BTI', 'BaseThingUpsert']}}}
		
		updates['$addToSet'] = {
			'_types': {
				'$each': self.documentTypes
			}
		}
		
		if not modifyAndReturn:
			# standard 'update'
			ret = self.collection.update( query, updates, upsert=upsert, safe=safeUpdate, multi=updateAllDocuments )
			if ret is None:
				return None
			if 'n' in ret:
				return ret['n']
		else:
			# findAndModify
			result = self.collection.find_and_modify(
				query=query,
				update=updates,
				upsert=upsert,
				new=returnAfterUpdate,
			)
			
			if result is None or len(result) == 0:
				return None
			else:
				return self._getNewInstance( result )
	
	def order_by( self, *fields ):
		kwargs = self._get_kwargs( )
		newOrderBy = self.orderBy[:]
		newOrderBy.extend( fields )
		kwargs['orderBy'] = newOrderBy
		return QuerySet( self.document, self.collection, **kwargs )
	
	def only( self, *fields ):
		kwargs = self._get_kwargs( )
		kwargs['fields'] = dict(self._fields or {}, **dict.fromkeys( fields, True ))
		return QuerySet( self.document, self.collection, **kwargs )
	
	def ignore( self, *fields ):
		kwargs = self._get_kwargs( )
		kwargs['fields'] = dict(self._fields or {}, **dict.fromkeys( fields, False ))
		return QuerySet( self.document, self.collection, **kwargs )

	def fields( self, **projections ):
		kwargs = self._get_kwargs( )
		kwargs['fields'] = dict(self._fields or {})
		for field, value in projections.iteritems( ):
			if '__' in field:
				fieldName, sep, projection = field.rpartition( '__' )
				if projection in PROJECTIONS:
					field = fieldName
					value = {'$%s' % projection: value}
			kwargs['fields'][field] = value
		return QuerySet( self.document, self.collection, **kwargs )
	
	def _do_find( self, **kwargs ):
		if 'sort' not in kwargs:
			sorting = sortListToPyMongo( self.orderBy )
			
			if len(sorting) > 0:
				kwargs['sort'] = sorting
		
		if self._fields is not None:
			kwargs['fields'] = self._fields

		if 'timeout' not in kwargs:
			kwargs['timeout'] = self.timeout

		if 'read_preference' not in kwargs and self.readPref is not None:
			kwargs['read_preference'] = self.readPref
		
		search = self._get_query( )

		if '_types' in search and 'fields' in kwargs and not kwargs['fields'].get( '_types' ) and all(kwargs['fields'].itervalues( )):
			kwargs['fields']['_types'] = True

		return self.collection.find( search, **kwargs )
	
	def _get_query( self, forUpsert=False ):
		search = self.query.toMongo( self.document )
		types = self.documentTypes
		if self.types:
			search['_types'] = {'$in': [subtype.__name__ for subtype in self.types]}
		elif len(types) > 1: # only filter when looking at a subclass
			if forUpsert:
				search['_types'] = {'$all':[self.document.__name__]} # filter by the type that was used
			else:
				search['_types'] = self.document.__name__ # filter by the type that was used
		return search
	
	def __iter__( self ):
		#print 'iter:', self.query.toMongo( self.document ), self.collection
		if self._savedItems is None:
			self._savedItems = self._do_find( )
		return (self._getNewInstance( item ) for item in self._savedItems.clone( ))
	
	def __getitem__( self, index ):
		if isinstance(index, int):
			getOne = True
			skip = index
			limit = 1
		elif isinstance(index, slice):
			getOne = False
			skip = index.start or 0
			limit = index.stop - skip if index.stop is not None else 0
			assert index.step is None, "Slicing with step not supported by mongorm"
		else:
			assert False, "item not an index"
		
		#print self.query.toMongo( self.document )
		#items = self.collection.find( self.query.toMongo( self.document ), skip=skip, limit=limit )
		items = self._do_find( skip=skip, limit=limit )
		
		if getOne:
			try:
				item = items[0]
			except IndexError:
				raise IndexError # re-raise our own index error
			document = self._getNewInstance( item )
			return document
		else:
			def _yieldItems():
				for item in items:
					document = self._getNewInstance( item )
					yield document
			return _yieldItems( )
	
	def first( self ):
		try:
			return self[0]
		except IndexError:
			return None
	
	def __call__( self, **search ):
		return self.filter( **search )
	
	def ensure_indexed( self ):
		"""Ensures that the most optimal index for the query so far is actually in the database.
		
		Call this whenever a query is deemed expensive."""
		
		indexKeys = []
		
		indexKeys.extend( self._queryToIndex( self.query.toMongo( self.document ) ) )
		
		indexKeys.extend( sortListToPyMongo( self.orderBy ) )
		
		uniqueKeys = []
		for key in indexKeys:
			if key not in uniqueKeys:
				uniqueKeys.append( key )
		
		self.collection.ensure_index( uniqueKeys )
		
		return self
	
	def _queryToIndex( self, query ):
		for key, value in query.iteritems( ):
			if key in ('$and', '$or'):
				for subq in value:
					for index in self._queryToIndex( subq ):
						yield index
			elif key.startswith( '$' ):
				continue # skip, it's a mongo operator and we can't search it?
			else:
				yield (key, pymongo.ASCENDING) # FIXME: work out direction better?
