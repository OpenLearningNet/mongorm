from mongorm.fields.BaseField import BaseField

from datetime import datetime

try:
	from pytz import utc
except ImportError:
	PYTZ = False
else:
	PYTZ = True

try:
	from iso8601 import parse_date, ParseError
except ImportError:
	ISO8601 = False
else:
	ISO8601 = True

class DateTimeField(BaseField):
	def fromPython( self, pythonValue, dereferences=[], modifier=None ):
		if ISO8601 and isinstance(pythonValue, basestring):
			try:
				pythonValue = parse_date( pythonValue )
			except ParseError:
				# oh well we tried
				pass

		if pythonValue is not None:
			if not isinstance(pythonValue, datetime):
				raise ValueError, "Value must be a datetime object not %r" % (pythonValue,)

			if PYTZ and pythonValue.tzinfo is not None:
				pythonValue = pythonValue.astimezone( utc )
				pythonValue = pythonValue.replace( tzinfo=None )

			# mongo doesn't handle microseconds
			pythonValue = pythonValue.replace( microsecond=(pythonValue.microsecond//1000)*1000 )

		return pythonValue
	
	def toPython( self, bsonValue ):
		if PYTZ and bsonValue is not None:
			bsonValue = utc.localize( bsonValue )
		return bsonValue
