kyoto = require 'kyoto'
leveled = require 'leveled'
ZSet = require './index'

l = leveled("/tmp/zset.level")

rand = (n) ->
  Math.floor Math.random() * n

aloop = (f, n=0, cb) ->
  if n > 0
    f ->
      aloop f, n-1, cb
  else
    cb()

aloop((cb) ->
  batch = new Date()
  aloop((cb) ->
    zset = new ZSet(l,  "this is about an id:and maybe another id:#{rand(10000)}")
    zset.incr "value #{rand(100)}", cb
  , 10000, ->
    console.log(10000 * 1000 / (new Date() - batch))
    cb()
  )
, 10000)


