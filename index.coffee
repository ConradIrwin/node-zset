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
    @locked = false
    @todo = []

  incr: (key, _cb) ->
    cb = (args...) =>
      @locked = false
      _cb(args...)
      if next = @todo.shift()
        @incr(next[0], next[1]) 

    if @locked
      return @todo.push([key, cb])
    else
      @locked = true
      @_incr(key, cb)

  _incr: (key, cb) ->
    key = key.toString()
    @db.getBulk [@summaryKey(), @scoreKey(key)], (err, result) =>
      return cb(err) if err


      summary = result[@summaryKey()]
      score = @numberify result[@scoreKey(key)]

      isNew = score == 0
      newScore = score + 1

      if !summary # new set!
        cardinality = 1
        total = 1
        topN = 1
        newSummary = @serializeSummary(total, cardinality, topN, @datum(newScore, key))

      else
        [total, cardinality, topN, top] = @parseSummary(summary)

        total += 1
        cardinality += 1 if isNew

        minimum = @numberify(top.substr(0, 8))
        if newScore < minimum && topN == @summarySize
          newSummary = @serializeSummary(total, cardinality, topN, top)

        else
          parsed = top.split("\x00")
          updated = false

          parsed = parsed.map (datum) =>
            if key == datum.substr(8)
              updated = true
              @datum(newScore, key)
            else
              datum

          parsed.push(@datum(newScore, key)) unless updated

          parsed.sort()
          parsed = parsed.slice(-@summarySize) if parsed.length > @summarySize
          newSummary = @serializeSummary(total, cardinality, parsed.length, parsed.join("\x00"))

      toSet = {}
      toSet[@summaryKey()] = newSummary
      toSet[@scoreKey(key)] = @stringify newScore
      @db.setBulk toSet, cb

  score: (key, cb) ->
    @db.get @scoreKey(key), (err, value) =>
      return cb(err) if err
      cb null, @numberify value

  summary: () ->
    summary = @db.get(@summaryKey())
    return {"total": 0, "cardinality": 0, "top": {}} unless summary
    [total, cardinality, topN, top] = @parseSummary(summary)

    output = {
      "total": total,
      "cardinality": cardinality,
      "top": {}
    }

    top.split("\x00").forEach (datum) =>
      output.top[datum.substr(8)] = @numberify(datum.substr(0, 8))

    output

  members: (n, cb) ->
    @db.matchPrefix "#{@name}:", n, (err, keys) =>
      return cb(err) if err
      cb null, keys.map (key) => key.substr("#{@name}:".length)

  total: (cb) ->
    @db.get @summaryKey(), (err, summary) =>
      return cb(err) if err
      if summary
        cb null, @parseTotal(summary)
      else
        cb null, 0

  cardinality: (cb) ->
    @db.get @summaryKey(), (err, summary) =>
      return cb(err) if err
      if summary
        cb null, @parseCardinality(summary)
      else
        cb null, 0

  top: (n, cb) ->
    @db.get @summaryKey(), (err, summary) =>
      return cb(err) if err
      ret = {}
      summary.substr(24).split("\x00").forEach (datum) =>
        ret[datum.substr(8)] = @numberify(datum.substr(0, 8))

      cb null, ret

  parseSummary: (summary) ->
    [@parseTotal(summary),
     @parseCardinality(summary),
     @parseTopN(summary),
     summary.substr(24)]

  parseTotal: (summary) ->
    @numberify summary.substr(0, 8)

  parseCardinality: (summary) ->
    @numberify summary.substr(8, 8)

  parseTopN: (summary) ->
    @numberify summary.substr(16, 8)

  serializeSummary: (total, cardinality, topN, top) ->
    return @stringify(total) + @stringify(cardinality) + @stringify(topN) + top

  stringify: (n) ->
    str = n.toString(16)
    "00000000".substr(0, 8 - str.length) + str

  numberify: (s) ->
    parseInt(s || 0, 16)

  datum: (score, key) ->
    @stringify(score) + key

  summaryKey: ->
    @name

  scoreKey: (key) ->
    "#{@name}:#{key}"

module.exports = ZSet
