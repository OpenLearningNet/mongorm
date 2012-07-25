from mongorm import *

class TestUnsafeKeys(Document):
	data = SafeDictField( )

def teardown_module(module):
	TestUnsafeKeys.objects.all( ).delete( )
	DocumentRegistry.clear( )

def check_safe_dict_with_data( data ):
	doc = TestUnsafeKeys( data=data )
	doc.save( )
	assert doc.data == data

def test_safe_dict_save_dot_key( ):
	check_safe_dict_with_data( {'.': ''} )

def test_safe_dict_save_dollar_key( ):
	check_safe_dict_with_data( {'$': ''} )

def test_safe_dict_save_nested_dot_key( ):
	check_safe_dict_with_data( {
		'': {
			'': {
				'.': None
			}
		}
	} )

def test_safe_dict_save_nested_dollar_key( ):
	check_safe_dict_with_data( {
		'': {
			'': {
				'$': None
			}
		}
	} )

def test_safe_dict_unicode( ):
	check_safe_dict_with_data( {
		u"$$$": None,
		u"...": None,
		u"déjà vu": True
	} )

def test_safe_dict_raw( ):
	check_safe_dict_with_data( {
		r".": None,
		r"$": None,
		r"s/DictField/Safe&/g": True
	} )

def test_safe_dict_query( ):
	assert TestUnsafeKeys.objects.filter( data__attributes__course__name='test' ).count( ) == 0
	TestUnsafeKeys( data={
		'attributes': {
			'course': {
				'name': 'test'
			}
		}
	} ).save( )
	assert TestUnsafeKeys.objects.filter( data__attributes__course__name='test' ).count( ) == 1

def test_safe_dict_unsafe_query( ):
	assert TestUnsafeKeys.objects.filter( Q( {'data__$$$__...__???': True} ) ).count( ) == 0
	TestUnsafeKeys( data={
		'$$$': {
			'...': {
				'???': True
			}
		}
	} ).save( )
	assert TestUnsafeKeys.objects.filter( Q( {'data__$$$__...__???': True} ) ).count( ) == 1
