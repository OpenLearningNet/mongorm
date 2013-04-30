from mongorm import connection
from mongorm.DocumentMetaclass import DocumentMetaclass

from mongorm.blackMagic import serialiseTypesForDocumentType

class BaseDocument(object):
	__metaclass__ = DocumentMetaclass
	__internal__ = True
	
	class DoesNotExist(Exception):
		pass
	
	def __init__( self, **kwargs ):
		self._is_lazy = False
		self._data = {}
		self._values = {}
		
		self._data['_types'] = serialiseTypesForDocumentType( self.__class__ )
		
		for name,value in kwargs.iteritems( ):
			setattr(self, name, value)
	
	def _fromMongo( self, data, overwrite=True ):
		self._is_lazy = True
		
		for (name,field) in self._fields.iteritems( ):
			dbField = field.dbField
			if dbField in data and ( overwrite or not name in self._values ):
				pythonValue = field.toPython( data[dbField] )
				setattr(self, name, pythonValue)
		
		return self

	def __setattr__( self, name, value ):
		try:
			field = self._fields[name]
		except KeyError:
			if name.startswith( '_' ) and hasattr(self, name):
				super(BaseDocument, self).__setattr__( name, value )
			else:
				raise AttributeError, "'%s' has no attribute '%s'" % (self.__class__.__name__, name)
		else:
			mongoValue = field.fromPython( value )
			self._data[field.dbField] = mongoValue
			pythonValue = None
			if mongoValue is not None:
				pythonValue = field.toPython( mongoValue )
			self._values[name] = pythonValue
	
	def __getattr__( self, name ):
		if (not self._values or name not in self._values) and self._is_lazy and \
			'_id' in self._data and self._data['_id'] is not None:
			# field is being accessed and the object is currently in lazy mode
			# may need to retrieve rest of document
			try:
				field = self._fields[name]
			except KeyError:
				raise AttributeError, "'%s' has no attribute '%s'" % (self.__class__.__name__, name)
			if field.dbField not in self._data:
				# field not retrieved from database! load whole document. weeee
				result = connection.getDatabase( )[self._collection].find_one( { '_id': self._data['_id'] } )
				if result is None:
					raise self.DoesNotExist
				self._fromMongo( result, overwrite=False )
				
				self._is_lazy = False
		
		default = None
		try:
			field = self._fields[name]
		except KeyError:
			raise AttributeError, "'%s' has no attribute '%s'" % (self.__class__.__name__, name)

		if field is not None:
			default = field.getDefault( )

		if not name in self._values:
			self._values[name] = default
		
		value = self._values.get( name )
		
		return value
	
	def _resyncFromPython( self ):
		# before we go any further, re-sync from python values where needed
		for (name,field) in self._fields.iteritems( ):
			if field._resyncAtSave:
				dbField = field.dbField
				pythonValue = getattr(self, name)
				self._data[dbField] = field.fromPython( pythonValue )
				#print 'resyncing', dbField, 'to', self._data[dbField]
		
