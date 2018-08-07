import pymongo
import warnings 

from mongorm.BaseDocument import BaseDocument
from mongorm.connection import getDatabase

from mongorm.errors import OperationError

class Document(BaseDocument):
	__internal__ = True
	__needs_primary_key__ = True
	__is_sharded__ = False
	
	def __eq__(self, other):
		if isinstance(other, self.__class__) and hasattr(other, self._primaryKeyField):
			assert self._primaryKeyField == other._primaryKeyField
			if getattr(self, self._primaryKeyField) == getattr(other, other._primaryKeyField):
				return True
		return False
	
	def __ne__(self, other):
		return not (self == other)
	
	def __init__( self, **kwargs ):
		super(Document, self).__init__( **kwargs )
	
	def save( self, forceInsert=False, **kwargs ):
		database = getDatabase( )
		collection = database[self._collection]
		
		self._resyncFromPython( )
		
		if '_id' in self._data and self._data['_id'] is None:
			del self._data['_id']

		# safe not supported in pymongo 3.0+, use w for write concern instead
		if 'safe' in kwargs:
			kwargs['w'] = 1 if kwargs['safe'] else 0
			del kwargs['safe']
			warnings.warn('{} safe not supported in pymongo 3.0+, use w for write concern instead'.format(collection.full_name), DeprecationWarning)

		try:
			if forceInsert:
				newId = collection.insert_one( self._data, **kwargs )
			else:
				if self.__is_sharded__:
					# This is specific to cosmosdb because it does not support collection.save when the collection is sharded.
					if self._shardKeyField != '_id' and self._shardKeyField not in self._data:
						raise OperationError('Could not find shard key in document data to save')
					
					if '_id' not in self._data:
						newId = collection.insert_one( self._data, **kwargs )
					else:
						_filter = {
							self._shardKeyField: self._data[self._shardKeyField],
							'_id': self._data['_id']
						}
						newId = collection.update_one(_filter, self._data, upsert=True, **kwargs)
				else:
					newId = collection.save( self._data, **kwargs )
		except pymongo.errors.OperationFailure, err:
			message = 'Could not save document (%s)'
			if u'duplicate key' in unicode(err):
				message = u'Tried to save duplicate unique keys (%s)'
			raise OperationError( message % unicode(err) )
		if newId is not None:
			setattr(self, self._primaryKeyField, newId)
		
		return self

	def __repr__( self ):
		return '<%s id=%s>' % (self.__class__.__name__, getattr(self, self._primaryKeyField))