assert = require("assert")
ZSet = require("../index")
gdbm = require("gdbm")

describe "zset", ->

  db = new gdbm.GDBM()
  db.open("/tmp/test.db", 0, gdbm.GDBM_NEWDB)

  describe "basics", ->
    zset = new ZSet(db, "example")
    before ->
      zset.incr("hello")
      zset.incr("world")
      zset.incr("hello")

    it "should track scores", ->
      assert.equal(zset.score("hello"), 2)
      assert.equal(zset.score("world"), 1)

    it "should be able to count total", ->
      assert.equal(zset.total(), 3)

    it "should be able to count cardinality", ->
      assert.equal(zset.cardinality(), 2)

    it "should be able to list members", ->
      assert.deepEqual(zset.members(), ["hello", "world"])

    it "should be able to summarize the top N", ->
      assert.deepEqual(zset.summary(), {
        "total": 3,
        "cardinality": 2,
        "top": {
          "hello": 2,
          "world": 1
        }
      })

  describe "utilized", ->

    zset = new ZSet(db, "example2", 5, 10)
    before ->
      for i in [1..20]
        for j in [0...i]
          zset.incr(i)

    it "should track the top N", ->
      assert.deepEqual(zset.summary(), {
        total: 210,
        cardinality: 20,
        top: {
          20: 20,
          19: 19,
          18: 18,
          17: 17,
          16: 16
        }
      })

    it "should track all the members", ->
      assert.deepEqual(zset.members(), [
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"
      ])

