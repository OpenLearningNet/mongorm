# -*- coding: utf8 -*-

from mongorm import *
from pymongo import ReadPreference
from pytest import raises

def teardown_module(module):
	DocumentRegistry.clear( )

def test_basic_equality( ):
	"""Tests field equality and nested field equality"""
	class Test(Document):
		data = DictField( )
		name = StringField( )
	
	# equality
	assert Q( name='c' ).toMongo( Test ) \
		== {'name': 'c'}
	assert Q( data__attributes__course__name='c' ).toMongo( Test ) \
		== {'data.attributes.course.name': 'c'}

def test_basic_comparisons( ):
	"""Tests field and nested field comparisons"""
	class Test(Document):
		data = DictField( )
		name = StringField( )
	
	# simple comparisons
	assert Q( name__lte='c' ).toMongo( Test ) \
		== {'name': {'$lte': 'c'}}
	assert Q( data__attributes__course__name__lte='c' ).toMongo( Test ) \
		== {'data.attributes.course.name': {'$lte': 'c'}}

def test_regex_comparisons( ):
	"""Tests field and nested field regex comparisons"""
	class Test(Document):
		data = DictField( )
		name = StringField( )
	
	# regex comparisons
	assert Q( data__attributes__course__name__icontains='c' ).toMongo( Test ) \
		== {'data.attributes.course.name': {'$options': 'i', '$regex': u'c'}}
	assert Q( name__icontains='c' ).toMongo( Test ) \
		== {'name': {'$options': 'i', '$regex': u'c'}}
		
def test_embedded_basic_comparisons( ):
	"""Tests nested field regex comparisons over an EmbeddedDocument boundary"""
	class Data(EmbeddedDocument):
		attributes = DictField( )
	class TestPage(Document):
		data = EmbeddedDocumentField(Data)

	# regex comparisons
	assert Q( data__attributes__course__name__lte='c' ).toMongo( TestPage ) \
		== {'data.attributes.course.name': {'$lte': 'c'}}
		
def test_embedded_regex_comparisons( ):
	"""Tests nested field regex comparisons over an EmbeddedDocument boundary"""
	class Data(EmbeddedDocument):
		attributes = DictField( )
	class TestPage(Document):
		data = EmbeddedDocumentField(Data)

	# regex comparisons
	assert Q( data__attributes__course__name__icontains='c' ).toMongo( TestPage ) \
		== {'data.attributes.course.name': {'$options': 'i', '$regex': u'c'}}

def test_multiple_or( ):
	class Test(Document):
		data = DictField( )
	
	query = '123'
	queryFilter = (
		Q(data__a__icontains=query) |
		Q(data__b__icontains=query) |
		Q(data__c__icontains=query)
	)
	
	assert queryFilter.toMongo( Test ) == {
		'$or': [
			{'data.a': {'$options': 'i', '$regex': '123'}},
			{'data.b': {'$options': 'i', '$regex': '123'}},
			{'data.c': {'$options': 'i', '$regex': '123'}}
		]
	}

def test_regex_escape( ):
	"""Tests to make sure regex matches work with values containing regex special characters"""
	class Test(Document):
		name = StringField( )
	
	# equality
	assert Q( name__icontains='test.test' ).toMongo( Test ) \
		== {'name': {'$options': 'i', '$regex': u'test\\.test'}}
	assert Q( name__iexact='test.test' ).toMongo( Test ) \
		== {'name': {'$options': 'i', '$regex': u'^test\\.test$'}}
	assert Q( name__iexact='test\\' ).toMongo( Test ) \
		== {'name': {'$options': 'i', '$regex': u'^test\\\\$'}}
	assert Q( name__iexact='test[abc]test' ).toMongo( Test ) \
		== {'name': {'$options': 'i', '$regex': u'^test\\[abc\\]test$'}}

def test_and_or( ):
	"""Tests to make sure 'or's can be embedded in 'and's"""
	connect( 'test_mongorm' )
	
	class TestAndOr(Document):
		name = StringField( )
		path = StringField( )
		index = ListField( StringField( ) )
	
	# using consecutive .filter calls
	assert TestAndOr.objects.filter( 
			Q( name__icontains='t' ) | Q( name__icontains='e' )
		).filter( name='123' ).query.toMongo( TestAndOr ) \
		== {'$or': [{'name': {'$options': 'i', '$regex': 't'}},
					{'name': {'$options': 'i', '$regex': 'e'}}],
			'name': u'123'}
	
	# using Q objects
	assert TestAndOr.objects.filter( 
			( Q( name__icontains='t' ) | Q( name__icontains='e' ) ) & Q( name='123' )
		).query.toMongo( TestAndOr ) \
		== {'$or': [{'name': {'$options': 'i', '$regex': 't'}},
					{'name': {'$options': 'i', '$regex': 'e'}}],
			'name': u'123'}
	
	# test ANDs
	assert TestAndOr.objects.filter(
		Q(index='123') &
		Q(index='456')
	).query.toMongo( TestAndOr ) \
	== {'$and': [
		{'index': '123'},
		{'index': '456'},
		]}
	
	# multiple ORs with embedded ANDs
	assert TestAndOr.objects.filter(
		Q(name__icontains='abc') |
		Q(path__icontains='def') |
		(
			Q(index='123') &
			Q(index='456')
		)
	).query.toMongo( TestAndOr ) \
	== {'$or': [{'name': {'$options': 'i', '$regex': 'abc'}},
				{'path': {'$options': 'i', '$regex': 'def'}},
				{'$and': [
					{'index': '123'},
					{'index': '456'},
					]},
				]}

def test_do_merge_or( ):
	"""Tests to make sure do_merge works with 'or' operator"""
	connect( 'test_mongorm' )

	class TestAndOr(Document):
		name = StringField( )
		path = StringField( )
		index = ListField( StringField( ) )

	query = Q( name="spam" ) | Q( name="eggs" )
	assert query.toMongo( TestAndOr ) == {
		'$or': [{'name': "spam"}, {'name': "eggs"}]
	}

	query &= Q( path=u"Green Midget Café" )
	assert query.toMongo( TestAndOr ) == {
		'$or': [{'name': "spam"}, {'name': "eggs"}],
		'path': u"Green Midget Café"
	}

	query |= Q( index='11' )
	assert query.toMongo( TestAndOr ) == {
		'$or': [{
			'$or': [{'name': "spam"}, {'name': "eggs"}],
			'path': u"Green Midget Café"
		}, {
			'index': '11'
		}]
	}

def test_do_merge_and( ):
	"""Tests to make sure do_merge works with 'and' operator"""
	connect( 'test_mongorm' )

	class TestAndOr(Document):
		name = StringField( )
		path = StringField( )
		index = ListField( StringField( ) )

	query = Q( name="spam" ) & Q( name="eggs" )
	assert query.toMongo( TestAndOr ) == {
		'$and': [
			{'name': "spam"}, {'name': "eggs"}
		]
	}

	query &= Q( path=u"Green Midget Café" )
	assert query.toMongo( TestAndOr ) == {
		'$and': [
			{'name': "spam"}, {'name': "eggs"}
		],
		'path': u"Green Midget Café"
	}

	query &= Q( index='123' ) & Q( index='456' )
	assert query.toMongo( TestAndOr ) == {
		'$and': [{
			'$and': [
				{'name': "spam"}, {'name': "eggs"}
			],
			'path': u"Green Midget Café"
		}, {
			'$and': [{'index': "123"}, {'index': "456"}]
		}]
	}

def test_referencefield_none( ):
	"""Make sure ReferenceField can be searched for None"""
	connect( 'test_mongorm' )

	class TestRef(Document):
		name = StringField( )

	class TestHolder(Document):
		ref = ReferenceField( TestRef )
	
	TestHolder.objects.delete( )
	TestHolder( ref=None ).save( )
	ref = TestRef( name='123' )
	ref.save( )
	TestHolder( ref=ref ).save( )
	
	assert TestHolder.objects.filter( ref=None ).query.toMongo( TestHolder ) \
		== {'ref': None}
	
	assert TestHolder.objects.filter( ref=None ).count( ) == 1
	assert TestHolder.objects.filter( ref=ref ).count( ) == 1
	assert TestHolder.objects.get( ref=ref ).ref.name == ref.name
	assert TestHolder.objects.count( ) == 2

def test_push( ):
	connect( 'test_mongorm' )

	class TestPush(Document):
		names = ListField( StringField( ) )
	
	assert TestPush.objects._prepareActions(
		push__names='123'
	) == {
		'$push': {'names': '123'}
	}
	
	assert TestPush.objects._prepareActions(
		pushAll__names=['123', '456']
	) == {
		'$pushAll': {'names': ['123', '456']}
	}

def test_in_operator( ):
	"""Tests in operator works with lists"""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	assert Q( name__in=[] ).toMongo( Test ) \
		== {'name': {'$in': []}}

	assert Q( name__in=['eggs', 'spam'] ).toMongo( Test ) \
		== {'name': {'$in': ['eggs', 'spam']}}

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.filter( name__in=[] ).count( ) == 0
	assert Test.objects.filter( name__in=['spam'] ).count( ) == 1
	assert Test.objects.filter( name__in=['eggs'] ).count( ) == 1
	assert Test.objects.filter( name__in=['spam', 'eggs'] ).count( ) == 2

def test_in_iter_operator( ):
	"""Tests in operator works with iterators"""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	assert Q( name__in={} ).toMongo( Test ) \
		== {'name': {'$in': []}}

	assert Q( name__in=set(['eggs', 'spam']) ).toMongo( Test ) \
		== {'name': {'$in': ['eggs', 'spam']}}

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	def test_gen( ):
		for item in ('eggs', 'spam'):
			yield item

	assert Test.objects.filter( name__in=() ).count( ) == 0
	assert Test.objects.filter( name__in={'spam': True} ).count( ) == 1
	assert Test.objects.filter( name__in=frozenset(['eggs']) ).count( ) == 1
	assert Test.objects.filter( name__in=test_gen() ).count( ) == 2

def test_in_operator_with_ref( ):
	"""Tests in operator works with references"""
	connect( 'test_mongorm' )

	class TestUser(Document):
		name = StringField( )

	class TestOrder(Document):
		user = ReferenceField( TestUser )
		breakfast = StringField( )

	# Clear objects so that counts will be correct
	TestUser.objects.all( ).delete( )
	TestOrder.objects.all( ).delete( )

	man = TestUser( name="Eric Idle" )
	wife = TestUser( name="Graham Chapman" )
	man.save( )
	wife.save( )

	assert TestUser.objects.filter( name__in=["Eric Idle", "Graham Chapman"] ).count( ) == 2

	TestOrder( user=man, breakfast="spam spam spam beans spam" ).save( )
	TestOrder( user=wife, breakfast="bacon and eggs" ).save( )

	assert TestOrder.objects.filter( user=man ).count( ) == 1
	assert TestOrder.objects.filter( user=wife ).count( ) == 1
	assert TestOrder.objects.filter( user__in=[man, wife] ).count( ) == 2

	TestOrder( user=man, breakfast="spam spam spam spam spam" ).save( )

	assert TestOrder.objects.filter( user__in=[man, wife] ).count( ) == 3
	assert TestOrder.objects.filter( breakfast__in=["spam spam spam", "bacon and eggs"] ).count( ) == 1
	assert TestOrder.objects.filter( breakfast__in=["spam spam spam spam spam", "bacon and eggs"] ).count( ) == 2
	assert TestOrder.objects.filter( breakfast__in=[
		"spam spam spam beans spam",
		"spam spam spam spam spam",
		"bacon and eggs"
	] ).count( ) == 3
	assert TestOrder.objects.filter( user__in=[wife], breakfast__in=["spam spam spam spam spam"] ).count( ) == 0
	assert TestOrder.objects.filter( user__in=[man], breakfast__in=["spam spam spam spam spam"] ).count( ) == 1
	assert TestOrder.objects.filter( user__in=[man, wife], breakfast__in=[
		"spam spam spam spam spam",
		"spam spam spam beans spam",
		"bacon and eggs"
	] ).count( ) == 3

def test_multiple_iteration( ):
	"""Tests multiple iterators work"""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Add some objects to the collection in case
	Test( name="John" ).save( )
	Test( name="Eric" ).save( )
	Test( name="Graham" ).save( )

	assert Test.objects.count( ) >= 3

	query = Test.objects.all( )
	it1 = iter(query)
	it2 = iter(query)

	for i in xrange(Test.objects.count( )):
		assert next(it1) == next(it2)

def test_secondary_read_pref( ):
	"""Tests read_preference works"""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Add some objects to the collection in case
	Test( name="John" ).save( )
	Test( name="Eric" ).save( )
	Test( name="Graham" ).save( )

	assert Test.objects.read_preference( 'secondary' ).count( ) >= 3
	assert Test.objects.filter( name="John" ).read_preference( ReadPreference.SECONDARY )[0].name == "John"

def test_slice_projection( ):
	"""Tests slice projection works"""
	connect( 'test_mongorm' )

	class TestArray(Document):
		names = ListField( StringField( ) )

	# Add some objects to the collection in case
	chaps = TestArray( names=["John", "Eric", "Graham"] )
	chaps.save( )

	assert TestArray.objects.filter( pk=chaps.id ).fields( names__slice=1 )[0].names == ["John"]
	assert TestArray.objects.fields( names__slice=1 ).get( pk=chaps.id ).names == ["John"]
	assert TestArray.objects.fields( names__slice=-1 ).get( pk=chaps.id ).names == ["Graham"]
	assert TestArray.objects.fields( names__slice=[1, 1] ).get( pk=chaps.id ).names == ["Eric"]
	assert TestArray.objects.fields( names__slice=4 ).get( pk=chaps.id ).names == ["John", "Eric", "Graham"]

def test_nin_operator( ):
	"""Tests nin (not in) operator works with lists"""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	assert Q( name__nin=[] ).toMongo( Test ) \
		== {'name': {'$nin': []}}

	assert Q( name__nin=['eggs', 'spam'] ).toMongo( Test ) \
		== {'name': {'$nin': ['eggs', 'spam']}}

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.filter( name__nin=[] ).count( ) == 2
	assert Test.objects.filter( name__nin=['spam'] ).count( ) == 1
	assert Test.objects.filter( name__nin=['eggs'] ).count( ) == 1
	assert Test.objects.filter( name__nin=['spam', 'eggs'] ).count( ) == 0

def test_nin_iter_operator( ):
	"""Tests nin (not in) operator works with iterators"""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	assert Q( name__nin={} ).toMongo( Test ) \
		== {'name': {'$nin': []}}

	assert Q( name__nin=set(['eggs', 'spam']) ).toMongo( Test ) \
		== {'name': {'$nin': ['eggs', 'spam']}}

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	def test_gen( ):
		for item in ('eggs', 'spam'):
			yield item

	assert Test.objects.filter( name__nin=() ).count( ) == 2
	assert Test.objects.filter( name__nin={'spam': True} ).count( ) == 1
	assert Test.objects.filter( name__nin=frozenset(['eggs']) ).count( ) == 1
	assert Test.objects.filter( name__nin=test_gen() ).count( ) == 0

def test_nin_operator_with_ref( ):
	"""Tests nin (not in) operator works with references"""
	connect( 'test_mongorm' )

	class TestUser(Document):
		name = StringField( )

	class TestOrder(Document):
		user = ReferenceField( TestUser )
		breakfast = StringField( )

	# Clear objects so that counts will be correct
	TestUser.objects.all( ).delete( )
	TestOrder.objects.all( ).delete( )

	man = TestUser( name="Eric Idle" )
	wife = TestUser( name="Graham Chapman" )
	man.save( )
	wife.save( )

	assert TestUser.objects.filter( name__nin=[] ).count( ) == 2
	assert TestUser.objects.filter( name__nin=["Eric Idle", "Graham Chapman"] ).count( ) == 0

	TestOrder( user=man, breakfast="spam spam spam beans spam" ).save( )
	TestOrder( user=wife, breakfast="bacon and eggs" ).save( )

	assert TestOrder.objects.filter( user__ne=man ).count( ) == 1
	assert TestOrder.objects.filter( user__ne=wife ).count( ) == 1
	assert TestOrder.objects.filter( user__nin=[man, wife] ).count( ) == 0

	TestOrder( user=man, breakfast="spam spam spam spam spam" ).save( )

	assert TestOrder.objects.filter( user__nin=[man, wife] ).count( ) == 0
	assert TestOrder.objects.filter( breakfast__nin=["spam spam spam", "bacon and eggs"] ).count( ) == 2
	assert TestOrder.objects.filter( breakfast__nin=["spam spam spam spam spam", "bacon and eggs"] ).count( ) == 1
	assert TestOrder.objects.filter( breakfast__nin=[
		"spam spam spam beans spam",
		"spam spam spam spam spam",
		"bacon and eggs"
	] ).count( ) == 0
	assert TestOrder.objects.filter( user__nin=[wife], breakfast__nin=["spam spam spam spam spam"] ).count( ) == 1
	assert TestOrder.objects.filter( user__nin=[man], breakfast__nin=["spam spam spam spam spam"] ).count( ) == 1
	assert TestOrder.objects.filter( user__nin=[man, wife], breakfast__nin=[
		"spam spam spam spam spam",
		"spam spam spam beans spam",
		"bacon and eggs"
	] ).count( ) == 0

def test_dict_queries( ):
	"""Tests dicts as a whole can be queried."""
	connect( 'test_mongorm' )

	class TestDict(Document):
		data = DictField( )

	assert Q( data={} ).toMongo( TestDict ) == {'data': {}}
	assert Q( data__gt={} ).toMongo( TestDict ) == {'data': {'$gt': {}}}

	# Clear objects so that counts will be correct
	TestDict.objects.all( ).delete( )

	TestDict( data={} ).save( )
	TestDict( data={"has": "something"} ).save( )

	assert TestDict.objects.all( ).count( ) == 2

	assert TestDict.objects.filter( data={} ).count( ) == 1
	assert TestDict.objects.filter( data__gt={} ).count( ) == 1

def test_subtype_queries( ):
	"""Tests querying objects based on their type."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		data = StringField( )

	class TestSubDocumentA(TestDocument):
		pass

	class TestSubDocumentB(TestDocument):
		pass

	class TestSubDocumentC(TestDocument):
		pass

	class TestOtherDocument(Document):
		data = StringField( )

	# Clear objects so that counts will be correct
	TestDocument.objects.all( ).delete( )

	TestSubDocumentA( data='spam' ).save( )
	TestSubDocumentB( data='spam' ).save( )
	TestSubDocumentC( data='spam' ).save( )

	assert TestDocument.objects.all( ).count( ) == 3
	assert TestSubDocumentA.objects.all( ).count( ) == 1
	assert TestSubDocumentB.objects.all( ).count( ) == 1
	assert TestSubDocumentC.objects.all( ).count( ) == 1

	assert TestDocument.objects.subtypes( TestSubDocumentA ).all( ).count( ) == 1
	assert TestDocument.objects.subtypes( TestSubDocumentB ).all( ).count( ) == 1
	assert TestDocument.objects.subtypes( TestSubDocumentC ).all( ).count( ) == 1

	assert TestDocument.objects.subtypes( TestSubDocumentA, TestSubDocumentB ).all( ).count( ) == 2
	assert TestDocument.objects.subtypes( TestSubDocumentA, TestSubDocumentB, TestSubDocumentC ).all( ).count( ) == 3

	with raises( TypeError ):
		TestOtherDocument.objects.subtypes( TestSubDocumentA ).count( )

	assert TestDocument.objects.subtypes( TestSubDocumentA ).all( ).order_by( 'data' ).count( ) == 1
