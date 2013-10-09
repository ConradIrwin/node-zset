assert = require("assert")
ZSet = require("../index")
kyoto = require("kyoto")
leveled = require("leveled")
Q = require 'q'
require('rimraf').sync("/tmp/aaa.db")

describe "zset", ->

  db = leveled("/tmp/aaa.db")

  describe "basics", ->
    zset = new ZSet(db, "example")
    before () ->
      Q().then(-> zset.incr("hello"))
         .then(-> zset.incr("world"))
         .then(-> zset.incr("hello"))

    it "should track scores", () ->
      zset.score("hello").then (score) ->
        assert.equal score, 2

        zset.score("world").then (score) ->
          assert.equal score, 1

    it "should be able to count total", () ->
      zset.total().then (total) ->
        assert.equal total, 3

    it "should be able to count cardinality", () ->
      zset.cardinality().then (cardinality) ->
        assert.equal cardinality, 2

    it "should be able to list members", () ->
      zset.members().then (members) ->
        assert.deepEqual members, ["hello", "world"]

    it "should be able to summarize the top N", () ->
      zset.top().then (top) ->
        assert.deepEqual top,
          hello: 2
          world: 1

  describe "utilized", ->
    zset = new ZSet(db, "example2", 5, 10)
    before ->
      todo = Q()
      done = 0
      for i in [1..20]
        for j in [0...i]
          do (i) ->
            todo = todo.then ->
              zset.incr(i)

      todo

    it "should track the top N", () ->
      zset.top().then (top) ->
        assert.deepEqual top,
          20: 20,
          19: 19,
          18: 18,
          17: 17,
          16: 16

    it "should track all the members", () ->
      zset.members().then (members) ->
        assert.deepEqual members,
          ["1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
           "2", "20", "3", "4", "5", "6", "7", "8", "9"]
