from mongorm.fields.DictField import DictField

from operator import methodcaller

def deepCoded( dictionary, coder ):
	coded = {}
	for key, value in dictionary.iteritems( ):
		if isinstance(key, basestring):
			# Keys have to be strings in mongo so this should always occur
			key = coder( key )
		if isinstance(value, dict):
			value = deepCoded( value, coder )
		coded[key] = value
	return coded

def encode( string ):
	if isinstance(string, unicode):
		string = string.encode( 'utf-8' )
	return string.encode( 'hex' )

def decode( string ):
	return string.decode( 'hex' ).decode( 'utf-8' )

class SafeDictField(DictField):
	def fromPython( self, *args, **kwargs ):
		result = super(SafeDictField, self).fromPython( *args, **kwargs )
		return deepCoded( result, encode )

	def toPython( self, *args, **kwargs ):
		result = super(SafeDictField, self).toPython( *args, **kwargs )
		return deepCoded( result, decode )

	def toQuery( self, pythonValue, dereferences=[] ):
		encodedDereferences = [encode( dereference ) for dereference in dereferences]
		return super(SafeDictField, self).toQuery( pythonValue, encodedDereferences )
