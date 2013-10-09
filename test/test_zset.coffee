assert = require("assert")
ZSet = require("../index")
kyoto = require("kyoto")
leveled = require("leveled")

describe "zset", ->

  db = leveled("/tmp/aaa.db")

  describe "basics", ->
    zset = null
    before (next) ->
      zset = new ZSet(db, "example")
      zset.incr "hello", (err) ->
        assert.ifError err
        zset.incr "world", (err) ->
          assert.ifError err
          zset.incr "hello", (err) ->
            assert.ifError err
            next()

    it "should track scores", (next) ->
      zset.score "hello", (err, value) ->
        assert.ifError err
        assert.equal value, 2
        zset.score "world", (err, value) ->
          assert.ifError err
          assert.equal value, 1
          next()

    it "should be able to count total", (next) ->
      zset.total (err, total) ->
        assert.ifError err
        assert.equal total, 3
        next()

    it "should be able to count cardinality", (next) ->
      zset.cardinality (err, cardinality) ->
        assert.ifError err
        assert.equal cardinality, 2
        next()

    it "should be able to list members", (next) ->
      zset.members -1, (err, members) ->
        assert.ifError err
        assert.deepEqual members, ["hello", "world"]
        next()

    it "should be able to summarize the top N", (next) ->
      zset.top -1, (err, top) ->
        assert.ifError err
        assert.deepEqual top,
          hello: 2
          world: 1
        next()

  describe "utilized", ->

    zset = null
    before (next) ->
      zset = new ZSet(db, "example2", 5, 10)
      done = 0
      for i in [1..20]
        for j in [0...i]
          zset.incr i, ->
            done += 1
            if done == 210
              next()

    it "should track the top N", (next) ->
      zset.top 5, (err, top) ->
        assert.ifError err
        assert.deepEqual top,
          20: 20,
          19: 19,
          18: 18,
          17: 17,
          16: 16
        next()

    it "should track all the members", (next) ->
      zset.members -1, (err, members) ->
        assert.ifError err
        assert.deepEqual members,
          ["1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
           "2", "20", "3", "4", "5", "6", "7", "8", "9"]
        next()
