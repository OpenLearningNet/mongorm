from mongorm import *
from bson.dbref import DBRef

def teardown_module(module):
	DocumentRegistry.clear( )
	
def test_update_dictfield( ):
	"""Tests to make sure updates are calculated correctly by dictfields"""
	class TestA(Document):
		data = DictField( )

	assert Q( { 'data__123': 'test' } ).toMongo( TestA, forUpdate=True ) == { 'data.123': 'test' }

	# children of a dictfield shouldn't be modified
	fieldName = 'data__123'
	value = {"XXX": "YYY"}
	assert Q( { fieldName: value } ).toMongo( TestA, forUpdate=True )[fieldName.replace('__', '.')] \
		== value
	value = ['test']
	assert Q( { fieldName: value } ).toMongo( TestA, forUpdate=True )[fieldName.replace('__', '.')] \
		== value
	
def test_update_types( ):
	"""Tests to make sure updates work with different types"""
	connect( 'test_mongorm' )
	
	class TestB(Document):
		dictval = DictField( )
		boolval = BooleanField( )
		stringval = StringField( )
		listval = ListField( StringField() )
		genericval = GenericReferenceField( )
	
	doc = TestB( )
	doc.save( )
	
	assert TestB.objects._prepareActions(
		set__boolval=True,
		set__stringval='test'
	) == {'$set': {'boolval': True, 'stringval': 'test'}}

	assert TestB.objects._prepareActions(
		set__listval=['a','b','c']
	) == {'$set': {'listval': ['a', 'b', 'c']}}
	
	assert TestB.objects._prepareActions(
		set__dictval__subkeybool=True,
		set__dictval__subkeystring='testing',
		set__dictval__subkeydict={'a':'b'},
	) == {'$set': { 'dictval.subkeybool': True,
	                'dictval.subkeydict': {'a': 'b'},
	                'dictval.subkeystring': 'testing'}}
	
	assert TestB.objects._prepareActions(
		set__genericval=doc
	) == {'$set': {'genericval': {'_types': ['TestB'], '_ref': DBRef('testb', doc.id)}}}

def test_push_pull_operators( ):
	"""Tests to make sure the push & pull operators work"""

	class TestPushPull(Document):
		values = ListField( StringField( ) )

	# Clear all objects so that counts will be correct
	TestPushPull.objects.all( ).delete( )

	# Check correct mongo is being produced

	assert TestPushPull.objects._prepareActions(
		push__values='spam'
	) == {'$push': {'values': 'spam'}}

	assert TestPushPull.objects._prepareActions(
		pushAll__values=['spam', 'eggs']
	) == {'$pushAll': {'values': ['spam', 'eggs']}}

	assert TestPushPull.objects._prepareActions(
		pull__values='spam'
	) == {'$pull': {'values': 'spam'}}

	assert TestPushPull.objects._prepareActions(
		pullAll__values=['spam', 'eggs']
	) == {'$pullAll': {'values': ['spam', 'eggs']}}

	# OK let's check with some real data

	a = TestPushPull( values=[] ).save( )
	assert a.values == []

	b = TestPushPull( values=['eggs'] ).save( )
	assert b.values == ['eggs']

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		push__values='spam'
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == ['spam']
	assert TestPushPull.objects.get( pk=b.id ).values == ['eggs', 'spam']

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		pushAll__values=[]
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == ['spam']
	assert TestPushPull.objects.get( pk=b.id ).values == ['eggs', 'spam']

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		pullAll__values=[]
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == ['spam']
	assert TestPushPull.objects.get( pk=b.id ).values == ['eggs', 'spam']

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		pull__values='spam'
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == []
	assert TestPushPull.objects.get( pk=b.id ).values == ['eggs']

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		pull__values='eggs'
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == []
	assert TestPushPull.objects.get( pk=b.id ).values == []

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		pushAll__values=['spam', 'eggs']
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == ['spam', 'eggs']
	assert TestPushPull.objects.get( pk=b.id ).values == ['spam', 'eggs']

	assert TestPushPull.objects.update(
		safeUpdate=True,
		updateAllDocuments=True,
		pullAll__values=['spam', 'eggs']
	) == 2

	assert TestPushPull.objects.get( pk=a.id ).values == []
	assert TestPushPull.objects.get( pk=b.id ).values == []

def test_setOnInsert( ):
	"""Tests to make sure $setOnInsert works"""

	class TestC(Document):
		name = StringField( )
		version = IntField( )

	# Clear objects to reset counts
	TestC.objects.all( ).delete( )

	assert TestC.objects._prepareActions(
		setOnInsert__name='spam',
		inc__version=1
	) == {'$setOnInsert': {'name': 'spam'}, '$inc': {'version': 1}}

	assert TestC.objects.filter( name='spam' ).count( ) == 0
	assert TestC.objects.filter( name='spam' ).update(
		safeUpdate=True,
		upsert=True,
		modifyAndReturn=True,
		setOnInsert__name='spam',
		inc__version=1
	) is None
	assert TestC.objects.filter( name='spam' ).count( ) == 1

	c = TestC.objects.filter( name='spam' ).update(
		safeUpdate=True,
		upsert=True,
		modifyAndReturn=True,
		returnAfterUpdate=True,
		setOnInsert__name='eggs',
		inc__version=1
	)
	assert c.name == 'spam'
	assert c.version == 2
