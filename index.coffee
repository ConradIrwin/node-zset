# Maintains a semi-sorted set in a hash table
# (designed to be used with GDBM or a similar system)
#
# The set supports one write operation:
#
# 1. .incr(key)  Increments the score of a member in the set.
#
# And maintains four properties:
#
# 1. .total()        The sum of all members scores.
# 2. .cardinality()  The number of unique member keys.
# 3. .topN()         The top <N> members in the set.
# 3. .members()      The list of members (in the order they were added)
#
class ZSet

  constructor: (@db, @name, @summarySize=10, @membersPerBucket=500) ->

  incr: (key, cb) ->
    key = key.toString()

    @db.increment @totalKey(), 1, 0, (err, newTotal) =>
      return cb(err) if err
      @db.increment @scoreKey(key), 1, 0, (err, newScore) =>
        return cb(err) if err
        isNew = (newScore == 1)

        @db.set @orderKey(key, newScore), "1", (err) =>
          return cb(err) if err

          if isNew
            @db.increment @cardinalityKey(), 1, 0, (err, newCardinality) =>
              return cb(err) if err
              cb()
          else
            @db.remove @orderKey(key, newScore - 1), (err) =>
              return cb(err) if err
              cb()

  score: (key, cb) ->
    @db.getInt @scoreKey(key), cb

  total: (cb) ->
    @db.getInt @totalKey(), cb

  cardinality: (cb) ->
    @db.getInt @cardinalityKey(), cb

  top: (n, cb) ->
    @db.matchPrefix @orderKeyPrefix(), n, (err, keys) =>
      return cb(err) if err
      ret = {}
      keys.forEach (key) =>
        ret[key.substr(@orderKeyPrefix().length + 8)] = @parseSortable(key.substr(@orderKeyPrefix().length, 8), 16)

      cb null, ret

  members: (n, cb) ->
    @db.matchPrefix @scoreKeyPrefix(), n, (err, keys) =>
      return cb(err) if err
      cb null, keys.map (key) => key.substr(@scoreKeyPrefix().length)

  scoreKey: (key) ->
    "#{@name}:#{key}"

  scoreKeyPrefix: () ->
    "#{@name}:"

  orderKey: (key, score) ->
    "#{@name}.#{@sortable(score)}#{key}"

  orderKeyPrefix: () ->
    "#{@name}."

  totalKey: (key) ->
    "#{@name}@total"

  cardinalityKey: (key) ->
    "#{@name}@cardinality"

  sortable: (number) ->
    str = (Math.pow(2, 32) - number).toString(16)
    "00000000".substr(0, 8 - str.length) + str

  parseSortable: (string) ->
    Math.pow(2, 32) - parseInt(string, 16)

module.exports = ZSet
