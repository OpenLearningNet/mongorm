
class Q(object):
	def __init__( self, _query=None, **search ):
		if _query is None:
			if 'pk' in search:
				search['id'] = search['pk']
				del search['pk']
			
			self.query = search
		else:
			self.query = _query
	
	def toMongo( self, document, forUpdate=False, modifier=None ):
		newSearch = {}
		for (name, value) in self.query.iteritems( ):
			if name in ['$or', '$and']:
				# mongodb logic operator - value is a list of Qs
				newSearch[name] = [ value.toMongo( document ) for value in value ]
				continue

			if name.startswith('$') and isinstance(value, basestring):
				newSearch[name] = value
				continue
			
			fieldName = name
			
			MONGO_COMPARISONS = ['gt', 'lt', 'lte', 'gte', 'exists', 'eq', 'ne', 'all', 'in', 'nin', 'elemMatch']
			REGEX_COMPARISONS = {
				'contains': ( '%s', '' ),
				'icontains': ( '%s', 'i' ),

				'iexact': ( '^%s$', 'i' ),

				'startswith': ( '^%s', '' ),
				'istartswith': ( '^%s', 'i' ),
				
				'endswith': ( '%s$', '' ),
				'iendswith': ( '%s$', 'i' ),
				
				'matches': ( None, '' ),
				'imatches': ( None, 'i' ),
			}
			ALL_COMPARISONS = MONGO_COMPARISONS + REGEX_COMPARISONS.keys()
			ARRAY_VALUE_COMPARISONS = ['all', 'in', 'nin']

			comparison = None
			dereferences = []
			if '__' in fieldName:
				chunks = fieldName.split( '__' )
				fieldName = chunks[0]

				comparison = chunks[-1]

				if comparison in ALL_COMPARISONS:
					dereferences = chunks[1:-1]
				else:
					# not a comparison operator
					dereferences = chunks[1:]
					comparison = None
			
			if fieldName not in document._fields:
				raise AttributeError, "%s does not contain the field '%s'" % (document.__name__, fieldName)
			
			field = document._fields[fieldName]
			if not forUpdate:
				if comparison in ARRAY_VALUE_COMPARISONS:
					searchValue = [field.toQuery( item, dereferences=dereferences ) for item in value]
				else:
					searchValue = field.toQuery( value, dereferences=dereferences )
			else:
				if comparison in ARRAY_VALUE_COMPARISONS:
					searchValue = [field.fromPython( item, dereferences=dereferences, modifier=modifier ) for item in value]
				else:
					searchValue = field.fromPython( value, dereferences=dereferences, modifier=modifier )

			targetSearchKey = '.'.join( [field.dbField] + dereferences)

			valueMapper = lambda value: value


			if comparison is not None:
				if comparison in REGEX_COMPARISONS:
					regex,options = REGEX_COMPARISONS[comparison]
					if regex is None:
						finalRegex = value
					else:
						safeValue = value
						regexReserved = [ '\\', '.', '*', '+' ,'^', '$', '[', ']', '?', '(', ')' ]
						for reserved in regexReserved:
							safeValue = safeValue.replace( reserved, '\\' + reserved )
						finalRegex = regex % safeValue
					valueMapper = lambda value: { '$regex': finalRegex, '$options': options }
					#pattern = regex % searchValue
					#print comparison, searchValue, targetSearchKey, pattern, options
					#newSearch[targetSearchKey] = { '$regex': pattern, '$options': options }
				else:
					valueMapper = lambda value: { '$'+comparison: value }
					#newSearch[targetSearchKey] = { '$'+comparison: searchValue }

			newSearch[targetSearchKey] = valueMapper(searchValue)

		return newSearch

	def __or__( self, other ):
		return self.do_merge( other, '$or' )

	def __and__( self, other ):
		if len( set( self.query.keys() ).intersection( other.query.keys() ) ) > 0:
			# if the 2 queries have overlapping keys, we need to use a $and to join them.
			return self.do_merge( other, '$and' )
		else:
			# otherwise we can just merge the queries together
			newQuery = {}
			newQuery.update( self.query )
			newQuery.update( other.query )
			return Q( _query=newQuery )
	
	def do_merge( self, other, op ):
		if len(self.query) == 0: return other
		if len(other.query) == 0: return self
		
		if op in self.query and len(self.query) == 1:
			items = self.query[op] + [other]
		elif op in other.query and len(self.query) == 1:
			items = other.query[op] + [self]
		else:
			items = [ self, other ]
		
		newQuery = { op: items }
		return Q( _query=newQuery )
