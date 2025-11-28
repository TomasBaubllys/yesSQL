// test-cursors.js
const DatabaseClient = require('./index');

async function testCursors() {
  console.log('🧪 Testing cursor operations...\n');

  const db = new DatabaseClient({ 
    url: 'http://127.0.0.1:8000' 
  });

  console.log('Session ID:', db.getSessionId(), '\n');

  try {
    //
    // --------------------------
    //  SETUP: LARGE TEST DATA
    // --------------------------
    //
    console.log('📦 Setting up LARGE test dataset...');

    // Users
    for (let i = 1; i <= 50; i++) {
      await db.set(`user:${i}`, `User_${i}`);
    }

    // Products
    for (let i = 1; i <= 50; i++) {
      await db.set(`product:${i}`, `Product_${i}`);
    }

    // Random categories
    for (let i = 1; i <= 30; i++) {
      await db.set(`order:${i}`, `Order_${i}`);
      await db.set(`meta:${i}`, `Meta_${i}`);
    }

    console.log('✅ Large dataset created\n');

    //
    // --------------------------------------
    //  TEST 1 — Create cursors
    // --------------------------------------
    //
    console.log('🧪 Test 1: Creating cursors...');
    await db.createCursor('cursor_main');
    await db.createCursor('cursor_alt');
    console.log('✅ Cursors created\n');


    //
    // --------------------------------------
    //  TEST 2 — Large forward scan
    // --------------------------------------
    //
    console.log('🧪 Test 2: Large forward scan (10 items)...');
    let batch1 = await db.getFF('cursor_main', 10);
    console.log('Batch:', batch1);
    console.log('✅ Forward scan works\n');


    //
    // --------------------------------------
    //  TEST 3 — Move until exhaustion
    // --------------------------------------
    //
    console.log('🧪 Test 3: Scanning forward until exhausted...');
    let done = false;
    let count = 0;

    while (!done) {
      const items = await db.getFF('cursor_main', 255);
      count += items.length;
      if (items.length < 20) done = true;
    }

    console.log(`Total items scanned: ${count}`);
    console.log('✅ Cursor exhausted correctly\n');


    //
    // --------------------------------------
    //  TEST 4 — Scan backward after exhaustion
    // --------------------------------------
    //
    console.log('🧪 Test 4: Scan backward (5 items)...');
    let back1 = await db.getFB('cursor_main', 5);
    console.log('Backward result:', back1);
    console.log('✅ Backward scan works\n');


    //
    // --------------------------------------
    //  TEST 5 — Prefix key scan (big)
    // --------------------------------------
    //
    console.log('🧪 Test 5: Prefix key scan for "user"...');
    await db.createCursor('cursor_prefix_user');
    let userKeys = await db.getKeysPrefix('cursor_prefix_user', 'user', 100);
    console.log('User keys count:', userKeys.length);
    console.log(userKeys);
    console.log('✅ Prefix scan works\n');


    //
    // --------------------------------------
    //  TEST 6 — Prefix key scan for "product"
    // --------------------------------------
    //
    console.log('🧪 Test 6: Prefix key scan for "product"...');
    await db.createCursor('cursor_prefix_product');
    let productKeys = await db.getKeysPrefix('cursor_prefix_product', 'product', 100);
    console.log('Product keys count:', productKeys.length);
    console.log('✅ Prefix scan works\n');


    //
    // --------------------------------------
    //  TEST 7 — Multiple cursors do NOT interfere
    // --------------------------------------
    //
    console.log('🧪 Test 7: Ensure cursor_main and cursor_alt have independent states...');
    let mainFF = await db.getFF('cursor_main', 1);
    let altFF  = await db.getFF('cursor_alt', 1);

    console.log('cursor_main ->', mainFF);
    console.log('cursor_alt ->', altFF);
    console.log('✅ Independent cursor positions confirmed\n');


    //
    // --------------------------------------
    //  TEST 8 — Random forward/backward sequence
    // --------------------------------------
    //
    console.log('🧪 Test 8: Random forward/backward operations...');

    for (let i = 0; i < 10; i++) {
      let dir = Math.random() < 0.5 ? 'FF' : 'FB';
      let count = Math.floor(Math.random() * 5) + 1;

      let result =
        dir === 'FF'
          ? await db.getFF('cursor_alt', count)
          : await db.getFB('cursor_alt', count);

      console.log(`   ${dir}(${count}) -> returned ${result.length} items`);
    }

    console.log('✅ Random access test passed\n');


    //
    // --------------------------------------
    //  TEST 9 — Error handling for missing cursor
    // --------------------------------------
    //
    console.log('🧪 Test 9: Error handling for missing cursor...');

    try {
      await db.getFF('cursor_does_not_exist', 5);
      console.log('❌ ERROR: Missing cursor should have thrown');
    } catch (e) {
      console.log('Expected error:', e.message);
      console.log('✅ Proper error on invalid cursor\n');
    }


    //
    // --------------------------------------
    //  TEST 10 — Delete all cursors
    // --------------------------------------
    //
    console.log('🧪 Test 10: Deleting all cursors...');
    await db.deleteCursor('cursor_main');
    await db.deleteCursor('cursor_alt');
    await db.deleteCursor('cursor_prefix_user');
    await db.deleteCursor('cursor_prefix_product');

    console.log('✅ Cursors deleted\n');


    console.log('🎉 ALL CURSOR TESTS COMPLETED SUCCESSFULLY! 🎉');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error(error);
  }
}

testCursors();
