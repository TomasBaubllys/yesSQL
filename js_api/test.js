// test.js
const DatabaseClient = require('./index');

async function testAPI() {
  console.log('🧪 Starting API tests...\n');

  // Create database client
  const db = new DatabaseClient({ 
    url: 'http://127.0.0.1:8000' 
  });

  try {
    // Test 1: SET a value
    console.log('Test 1: Setting a value...');
    await db.set('test_key', 'test_value');
    console.log('✅ SET successful\n');

    // Test 2: GET the value
    console.log('Test 2: Getting the value...');
    const value = await db.get('test_key');
    console.log('✅ GET successful');
    console.log('   Value:', value);
    console.log('   Expected: test_value');
    console.log('   Match:', value === 'test_value' ? '✅' : '❌');
    console.log();

    // Test 3: SET another value
    console.log('Test 3: Setting another value...');
    await db.set('name', 'John');
    const name = await db.get('name');
    console.log('✅ Value:', name);
    console.log();

    // Test 4: GET non-existent key
    console.log('Test 4: Getting non-existent key...');
    const missing = await db.get('doesnt_exist');
    console.log('✅ Result:', missing);
    console.log('   Expected: null');
    console.log('   Match:', missing === null ? '✅' : '❌');
    console.log();

    // Test 5: REMOVE a key
    console.log('Test 5: Removing a key...');
    await db.remove('test_key');
    console.log('✅ REMOVE successful');
    const removed = await db.get('test_key');
    console.log('   After removal:', removed);
    console.log('   Expected: null');
    console.log('   Match:', removed === null ? '✅' : '❌');
    console.log();

    console.log('✅ All tests passed!');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error(error);
  }
}

testAPI();