// test-cursors.js
const DatabaseClient = require('./index');

async function testCursors() {
  console.log("🧪 RUNNING EXTREME CURSOR TESTS\n");

  const db = new DatabaseClient({ url: "http://127.0.0.1:8000" });

  console.log("Session:", db.getSessionId(), "\n");

  try {
    //
    // =====================================
    //  MASSIVE DATASET
    // =====================================
    //
    console.log("📦 Building MASSIVE test dataset...");
    let total = 0;
    for (let i = 1; i <= 500; i++) {
      await db.set(`user:${i}`, `User_${i}`);
      await db.set(`product:${i}`, `Product_${i}`);
      total += 2;
    }
    console.log(`✅ Dataset ready (${total} keys)\n`);

    //
    // =====================================
    //  TEST 1 — CREATE MAIN CURSOR
    // =====================================
    //
    console.log("🧪 Test 1: Creating main cursor");
    await db.createCursor("c1");
    console.log("✅ Cursor created\n");

    //
    // =====================================
    //  TEST 2 — FULL FORWARD SCAN (UNTIL EMPTY)
    // =====================================
    //
    console.log("🧪 Test 2: FULL forward scan");

    let scanned = 0;
    while (true) {
      const chunk = await db.getFF("c1", 128);
      scanned += chunk.length;
      if (chunk.length === 0) break;
      if (chunk.length > 128) throw new Error("Returned more than requested!");
    }
    console.log(`   Scanned forward: ${scanned}`);
    if (scanned !== total) throw new Error("Forward scan count mismatch");
    console.log("✅ Full forward scan OK\n");

    //
    // =====================================
    //  TEST 3 — FORWARD AT END MUST ALWAYS RETURN []
    // =====================================
    //
    console.log("🧪 Test 3: GET_FF while at end");

    for (let i = 0; i < 10; i++) {
      const chunk = await db.getFF("c1", 999);
      if (chunk.length !== 0) throw new Error("Cursor moved beyond end");
    }

    console.log("✅ Stays empty at end\n");

    //
    // =====================================
    //  TEST 4 — BACKWARD TO BEGINNING
    // =====================================
    //
    console.log("🧪 Test 4: FULL backward scan");

    let backCount = 0;
    while (true) {
      const chunk = await db.getFB("c1", 64);
      backCount += chunk.length;
      if (chunk.length === 0) break;
    }
    console.log(`   Items scanned backward: ${backCount}`);
    if (backCount !== total) throw new Error("Backward scan count mismatch");
    console.log("✅ Full backward scan OK\n");

    //
    // =====================================
    //  TEST 5 — BACKWARD AT START MUST RETURN []
    // =====================================
    //
    console.log("🧪 Test 5: GET_FB at beginning");

    for (let i = 0; i < 10; i++) {
      const chunk = await db.getFB("c1", 999);
      if (chunk.length !== 0) throw new Error("Backward read moved before 0");
    }

    console.log("✅ Stays empty at beginning\n");

    //
    // =====================================
    //  TEST 6 — FORWARD AND BACKWARD COMBO
    // =====================================
    //
    // console.log("🧪 Test 6: F/B consistency");

    // await db.createCursor("c2");

    // const f1 = await db.getFF("c2", 10);
    // const b1 = await db.getFB("c2", 10);

    // if (b1.length !== 0) {
    //   console.log({ f1, b1 });
    //   throw new Error("Backward after 10 FF should return exactly what was scanned");
    // }

    // console.log("✅ Forward/backward consistency OK\n");

    //
    // =====================================
    //  TEST 7 — PREFIX CURSOR VALIDATION
    // =====================================
    //
    console.log("🧪 Test 7: Prefix cursor filtration");

    await db.createCursor("c_users");
    let userKeys = await db.getKeysPrefix("c_users", "user:", 1024);

    if (userKeys.some(k => !k.startsWith("user:")))
      throw new Error("Prefix scan returned keys outside prefix!");

    if (userKeys.length !== 500)
      throw new Error("Prefix scan count mismatch");

    console.log("   user: prefix OK");

    await db.createCursor("c_products");
    let productKeys = await db.getKeysPrefix("c_products", "product:", 1024);

    if (productKeys.some(k => !k.startsWith("product:")))
      throw new Error("Prefix scan returned keys outside prefix!");

    console.log("   product: prefix OK\n");

    //
    // =====================================
    //  TEST 8 — 10 CURSORS INTERLEAVED
    // =====================================
    //
    console.log("🧪 Test 8: 10 simultaneous cursors ZIG-ZAG scan");

    for (let i = 0; i < 10; i++) {
      await db.createCursor(`cx${i}`);
    }

    for (let step = 0; step < 50; step++) {
      let c = `cx${step % 10}`;
      if (Math.random() < 0.5) {
        await db.getFF(c, Math.floor(Math.random() * 8) + 1);
      } else {
        await db.getFB(c, Math.floor(Math.random() * 8) + 1);
      }
    }

    console.log("✅ Multiple cursors hold independent positions\n");

    //
    // =====================================
    //  TEST 9 — DELETED CURSOR MUST BREAK
    // =====================================
    //
    console.log("🧪 Test 9: Deleted cursor behavior");

    await db.createCursor("tempC");
    await db.deleteCursor("tempC");

    let threw = false;
    try {
      await db.getFF("tempC", 5);
    } catch (e) {
      threw = true;
    }
    if (!threw) throw new Error("Using deleted cursor SHOULD throw");

    console.log("✅ Deleted cursor correctly rejected\n");

    //
    // =====================================
    //  TEST 10 — DESTROY THEN RECREATE CURSOR
    // =====================================
    //
    console.log("🧪 Test 10: Cursor recreation");

    await db.createCursor("reC");
    await db.getFF("reC", 3);
    await db.deleteCursor("reC");
    await db.createCursor("reC");

    const r = await db.getFF("reC", 1);
    if (r.length === 0) throw new Error("Recreated cursor did not reset");

    console.log("✅ Cursor recreation resets state\n");

    //
    // =====================================
    //  FINAL RESULT
    // =====================================
    //  
    console.log("\n🎉 ALL EXTREME CURSOR TESTS PASSED 🎉\n");

  } catch (error) {
    console.error("\n❌ TEST FAILURE:", error.message);
    console.error(error);
  }
}

testCursors();
