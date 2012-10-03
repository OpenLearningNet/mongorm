import mongorm

def test_connect( ):
	mongorm.connect( 'test_mongorm' )
	assert mongorm.connection.getDatabase( ).name == 'test_mongorm'

def test_reconnect( ):
	mongorm.connect( 'test_mongorm' )
	assert mongorm.connection.getDatabase( ).name == 'test_mongorm'

	mongorm.connect( 'test_mongorm_2' )
	assert mongorm.connection.getDatabase( ).name == 'test_mongorm_2'
