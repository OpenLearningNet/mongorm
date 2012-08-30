from mongorm.fields.BaseField import BaseField

from datetime import datetime

try:
	from pytz import UTC
except ImportError:
	UTC = None

class DateTimeField(BaseField):
	def fromPython( self, pythonValue, dereferences=[], modifier=None ):
		if pythonValue is not None and not isinstance(pythonValue, datetime):
			raise ValueError, "Value must be a datetime object not %r" % (pythonValue,)

		if UTC is not None and pythonValue is not None and pythonValue.tzinfo is not None:
			pythonValue = pythonValue.astimezone( UTC )
			pythonValue = pythonValue.replace( tzinfo=None )

		return pythonValue
	
	def toPython( self, bsonValue ):
		if UTC is not None and bsonValue is not None:
			bsonValue = UTC.localize( bsonValue )
		return bsonValue
