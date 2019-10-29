import binascii

from builtins import str, bytes
from past.builtins import basestring
from mongorm.fields.DictField import DictField

from collections import deque
from operator import methodcaller
from copy import deepcopy

def deepCoded( dictionary, coder ):
	dictionary = deepcopy( dictionary ) # leave the original intact
	toCode = deque( [dictionary] )
	while toCode:
		nextDictionary = toCode.popleft( )
		for key, value in list(nextDictionary.items( )): # can't be iteritems as we're changing the dict
			if isinstance(key, basestring):
				# Keys have to be strings in mongo so this should always occur
				del nextDictionary[key]
				nextDictionary[coder( key )] = value
			if isinstance(value, dict):
				toCode.append( value )
	return dictionary

def encode( string ):
	if isinstance(string, str):
		string = string.encode( 'utf-8' )
	return bytes(binascii.hexlify(string)).decode('utf-8')

def decode( string ):
	return bytes(binascii.unhexlify(string)).decode('utf-8')

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
