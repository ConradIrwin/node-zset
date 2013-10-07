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

  incr: (key) ->
    key = key.toString()
    summary = @db.fetch(@summaryKey())
    score = @score(key)
    isNew = score == 0

    newScore = score + 1

    @db.store(@scoreKey(key), @stringify(newScore))

    if !summary # new set!
      cardinality = 1
      total = 1
      topN = 1

      @db.store(@summaryKey(), @serializeSummary(total, cardinality, topN, @datum(newScore, key)))
      @db.store(@membersKey(cardinality), key)
      return

    [total, cardinality, topN, top] = @parseSummary(summary)

    total += 1
    if isNew
      cardinality += 1
      members = @db.fetch(@membersKey(cardinality))
      if members
        members += "\x00#{key}"
      else
        members = key

      @db.store(@membersKey(cardinality), members)

    minimum = @numberify(top.substr(0, 8))
    if newScore < minimum && topN == @summarySize
      @db.store(@summaryKey(), @serializeSummary(total, cardinality, topN, top))

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
      @db.store(@summaryKey(), @serializeSummary(total, cardinality, parsed.length, parsed.join("\x00")))

  score: (key) ->
    @numberify @db.fetch(@scoreKey(key))

  summary: () ->
    summary = @db.fetch(@summaryKey())
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

  members: () ->
    members = []
    i = 1
    while more = @db.fetch(@membersKey(i))
      members = members.concat(more.split("\x00"))
      i += @membersPerBucket

    members

  total: () ->
    summary = @db.fetch(@summaryKey())
    return 0 unless summary
    @parseTotal(summary)

  cardinality: () ->
    summary = @db.fetch(@summaryKey())
    return 0 unless summary
    @parseCardinality(summary)

  top: () ->
    @summary().top

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

  membersKey: (cardinality) ->
    "#{@name}.#{Math.floor(cardinality / @membersPerBucket)}"

module.exports = ZSet
