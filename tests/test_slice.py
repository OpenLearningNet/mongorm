from mongorm import *

def teardown_module(module):
	DocumentRegistry.clear( )

def test_getitem_one( ):
	"""Tests you can get one item from query."""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.count( ) == 2
	assert Test.objects.order_by( 'name' )[0].name == 'eggs'
	assert Test.objects.order_by( 'name' )[1].name == 'spam'

def test_getitem_multiple( ):
	"""Tests you can get multiple items from query."""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.count( ) == 2
	assert [x.name for x in Test.objects.order_by( 'name' )[0:2]] == ['eggs', 'spam']

def test_getitem_no_start( ):
	"""Tests you can get items from query without specifying a start index."""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.count( ) == 2
	assert [x.name for x in Test.objects.order_by( 'name' )[:2]] == ['eggs', 'spam']

def test_getitem_no_end( ):
	"""Tests you can get items from query without specifying an end index."""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.count( ) == 2
	assert [x.name for x in Test.objects.order_by( 'name' )[0:]] == ['eggs', 'spam']

def test_getitem_no_indices( ):
	"""Tests you can get items from query without specifying any index."""
	connect( 'test_mongorm' )

	class Test(Document):
		name = StringField( )

	# Clear objects so that counts will be correct
	Test.objects.all( ).delete( )

	Test( name='spam' ).save( )
	Test( name='eggs' ).save( )

	assert Test.objects.count( ) == 2
	assert [x.name for x in Test.objects.order_by( 'name' )[:]] == ['eggs', 'spam']
