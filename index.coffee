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
Q = require 'q'

class ZSet

  constructor: (@db, @name, @summarySize=10, @membersPerBucket=500) ->
    @inProgress = Q()

  incr: (key, _cb) ->
    # Ensure there can only be one event in progress at a time
    @inProgress = @inProgress.then =>
      @_incr key

  _incr: (key, cb) ->
    key = key.toString()
    summary = null
    score = null
    @get(@summaryKey()).then((result) =>
      summary = result
      if summary
        @get(@scoreKey(key)).then (result) =>
          score = result 
    ).then =>
      score = @numberify score

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
          parsed = top.split("\x01")
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
          newSummary = @serializeSummary(total, cardinality, parsed.length, parsed.join("\x01"))

      @write @db.batch()
        .put(@summaryKey(), newSummary)
        .put(@scoreKey(key), @stringify newScore)

  score: (key, cb) ->
    @get(@scoreKey(key)).then @numberify

  summary: () ->
    @get(@summaryKey()).then (summary) =>
      return {"total": 0, "cardinality": 0, "top": {}} unless summary
      [total, cardinality, topN, top] = @parseSummary(summary)

      output = {
        "total": total,
        "cardinality": cardinality,
        "top": {}
      }

      top.split("\x01").forEach (datum) =>
        output.top[datum.substr(8)] = @numberify(datum.substr(0, 8))

      output

  members: () ->
    @range("#{@name}:", "#{@name};").then (keys) =>
      Object.keys(keys).map (key) => key.substr("#{@name}:".length)

  total: () ->
    @get(@summaryKey()).then (summary) =>
      if summary
        @parseTotal(summary)
      else
        0

  cardinality: () ->
    @get(@summaryKey()).then (summary) =>
      if summary
        @parseCardinality(summary)
      else
        0

  top: () ->
    @get(@summaryKey()).then (summary) =>
      ret = {}
      summary.substr(24).split("\x01").forEach (datum) =>
        ret[datum.substr(8)] = @numberify(datum.substr(0, 8))
      ret

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

  get: (key) ->
    Q.nmcall(@db, 'get', key).fail (err) ->
      if err.toString() == "NotFound: "
        null
      else
        throw err

  write: (batch) ->
    Q.nmcall(batch, 'write')

  range: (from, to) ->
    Q.nmcall(@db, 'range', from, to)

module.exports = ZSet
