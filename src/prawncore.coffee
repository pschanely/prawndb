fs = require('fs')
Q = require('Q')
util = require('util')
_ = require('underscore')

# Query Translation:
# ['&', x1, ..]
# ['|', x1, ..]
# ['!', x1, ..]
# ['[]', x, y]
# ['()', x, y]
# ['in', x1, ..]
# { k1:v1, k2,:v2, .. }
# 'literal string'
# 42
# true
# null

# doc id + term list ... (usually there will be only one!)

handle = (req, res) ->
    req.method
    req.writeHead(200, {'Content-Type': 'application/json'})
    req.end()

rangeQueryFactory = (minIsInclusive, maxIsInclusive) ->
    null

_queryOps = {
        '&': AndQuery,
        '|': OrQuery,
        '!': NotQuery,
        'in': InQuery,
        '[]': rangeQueryFactory(true, true),
        '[)': rangeQueryFactory(true, false),
        '(]': rangeQueryFactory(false, true),
        '()': rangeQueryFactory(false, false),
        }

_NULL = 10
_FALSE = 20
_NUMBER = 40
_STRING = 50
_TRUE = 60

encodeNumber = (buf, offset) ->
    # Invert the negation flag itself to put positives above negatives:
    headByte = buf.readUInt8(offset)
    if (headByte & 0x80) != 0
        # if it's a negative, invert the other bits so that a bytewise 
        # lexiographic sort puts big negatives below small negatives
        buf.writeUInt32(buf.readUInt32(buf, offset) ^ 0x80000000, buf, offset)
        offset += 8
        buf.writeUInt32(buf.readUInt32(buf, offset) ^ 0x80000000, buf, offset)
    else
        buf.writeUInt8(headByte ^ 0x80, offset)

decodeNumber = (buf, offset) ->
    headByte = buf.readUInt8(offset)
    if (headByte & 0x80) == 0
        buf.writeUInt32(buf.readUInt32(buffer, offset)^0x80000000, buffer, offset)
        offset += 8
        buf.writeUInt32(buf.readUInt32(buffer, offset)^0x80000000, buffer, offset)
    else
        buf.writeUInt8(headByte ^ 0x80, offset)

class UserError
# extends Error

termifyObject = (obj, prefix, terms) ->
    if not prefix
        prefix = new PositionalBuffer(new Buffer(32))
        terms = []
    if util.isArray(obj)
        for item in obj
            terms.push(termifyObject(item, prefix, terms))
    else if typeof obj == 'object'
        for key, val of obj
            curPrefix = prefix.copy()
            curPrefix.writeVint(key.length)
            curPrefix.writeString(key)
            # TODO if this is atomic (the common case), inline here to avoid copies
            termifyObject(val, curPrefix, terms)
    else
        termBuffer = prefix.copy()
        termBuffer.writeVint(0)
        termBuffer.encodeAtomic(obj)
        terms.push(termBuffer.done())
    return terms

parseQueryStruct = (prefix, queryStruct, bag) ->
    if util.isArray(queryStruct)
        if queryStruct.length == 0
            throw new UserError('Invalid empty array in query structure')
        queryClass = _queryOps[queryStruct[0]]
        if queryClass instanceof RangeQueryFactory
            new queryClass(bag, encodeAtomic(queryStruct[1]), encodeAtomic(queryStruct[2]))
        else
            new queryClass(bag, [parseQueryStruct(prefix, item, bag) for item in queryStruct[1..]])
    else if typeof queryStruct == 'object'
        prefix += '.'
        queries = [parseQueryStruct(prefix+key, val, bag) for key, val of queryStruct]
        new AndQuery(bag, queries)
    else
        new EqQuery(bag, encodeAtomic(queryStruct))

readBuffer = (fd, ptr, sz) ->
    buf = new Buffer(sz)
    if sz == 0
        return Q.fcall(->buf)
    else
        console.log 'readbuff @', ptr
        Q.nfcall(fs.read, fd, buf, 0, sz, ptr).then(-> buf)

setFile = (fd, content) ->
    d = Q.nfcall(fs.write, fd, content, 0, content.length, 0)
    d.then(-> Q.nfcall(fs.fsync, fd))

termsToObject = (terms) ->
    console.log 'terms to object ', terms
    obj = {}
    for term in terms
        buffer = new PositionalBuffer term
        objPtr = obj
        len = buffer.readVint()
        while true
            key = buffer.readString(len)
            len = buffer.readVint()
            if len == 0
                objPtr[key] = buffer.decodeAtomic()
                break
            val = objPtr[key]
            if not val?
                val = objPtr[key] = {}
            objPtr = val
    return obj

serializeResults = (query, results) ->
    if not results?
        results = []
    docId = query.doc()
    if docId == -1
        throw new Error('cannot serialize an uninitialized query')
    if docId < Number.MAX_VALUE
        results.push(termsToObject(query.terms()))
        Q.when(query.next()).then(-> serializeResults(query, results))
    else
        Q.fcall(-> results)

sendResults = (query, response) ->
    docId = query.doc()
    if docId < Number.MAX_VALUE
        response.write(JSON.stringify(termsToObject(query.terms())))
        Q.when(query.next()).then(-> serializeResults(query, response))
        query.next()
    else
        response.end()

digestSamples = (terms, numBuckets, digest) ->
    terms = terms.sort()
    termsPerBucket = terms.length / numBuckets
    for index in [1 ... numBuckets]
        digest.push(terms[index * termsPerBucket])

collectTerms = (query, count) ->
    terms = []
    while query.next() < Number.MAX_VALUE
        for term in query.terms()
            terms.push(term)
        if terms.length >= sampleSize
            return terms
        
sampleAllTerms = (query, count) ->
    numLists = Math.floor(Math.sqrt(count) / 5) + 1
    sqrtOfListLength = Math.floor(Math.sqrt(count / numLists))
    numSampleTerms = numLists * 10 + 100
    digests = []
    for _ in [0...sqrtOfListLength]
        digestSamples(collectTerms(query, sqrtOfListLength), numLists, digests)
    results = []
    digestSamples(digests, numLists, results)
    return results

balanceItems = (items, targetCountPerTier) ->
    numTiers = 1
    while items.length / Math.pow(targetCountPerTier, numTiers) > 1.0
        numTiers += 1
    # take the Nth root of the item count, where N is the tree depth
    actualCountPerTier = Math.pow(items.length, 1 / numTiers)
    return balanceTier(items, actualCountPerTier)

balanceTier = (items, actualCountPerTier) ->
    if items.length <= actualCountPerTier
        return items
    bucketSize = items.length / actualCountPerTier
    result = new Array(actualCountPerTier)
    for bucket in [0 ... actualCountPerTier]
        subitems = items[bucket * bucketSize : bucket * (bucketSize + 1)]
        result[bucket] = balanceTier(subitems)
    return result

class PrawnIndex
    constructor: (options) ->
        if options.headerFd and options.dataFd
            [@headerFd, @dataFd] = [options.headerFd, options.dataFd]
        else if options.headerFile and options.dataFile
            [@headerFile, @dataFile] = [options.headerFile, options.dataFile]
        else if options.path
            @headerFile = path.join(options.path, 'header')
            @dataFile = path.join(options.path, 'data')
        else
            throw new Exception('An index location must be specified')
        
    ensureOpen: ->
        if @headerFd is undefined
            return Q.all([
                Q.nfcall(fs.open, @headerFile, 'r+'),
                Q.nfcall(fs.open, @dataFile, 'r'),
            ]).then((h, d) =>
                @headerFd = h
                @dataFd = d
            )
        
    create: ->
        tree = JSON.stringify([{t:'', p:0, s:0, c:0, x:''}])
        headrec = JSON.stringify({ptr:0, sz:tree.length, m:1})
        return Q.when(this.ensureOpen()).then(=> Q.all([
            setFile(@headerFd, new Buffer(headrec)),
            setFile(@dataFd, new Buffer(tree))
        ])).then(=>
            this
            )
        
    init: ->
        buf = new Buffer(1024)
        Q.nfcall(fs.fstat, @headerFd).then((headerStats) =>
            @headerSz = headerStats.size
            pos = @headerSz - 1024
            Q.nfcall(fs.read, @headerFd, buf, 0, 1024, if pos < 0 then 0 else pos)
        ).then(([numRead, _buffer]) =>
            lines = buf.toString('ascii', 0, numRead).split('\n')
            @header = JSON.parse(lines[lines.length-1])
            this.loadRootTree()
        ).then( => this)
        
    loadRootTree:() ->
        return readBuffer(@dataFd, @header.ptr, @header.sz).then((buf) =>
            @rootTree = JSON.parse(buf.toString('ascii'))
        )
        # root tree is an ordered list of [ {t[erm]:,p[tr]:,s[ize]:,c[ount]:,[prefi]x:,m[ax_docid]}, ]
        # TODO: if the file is bigger than the header indicates, it should be truncated to the given length

    appendAll:(fd, data, position) ->
        this.writeAllData(fd, data, 0, position)
        
    writeAllData:(fd, data, offset, position) ->
        remaining = data.length - offset
        #console.log 'write all fs.write ', data, ' at ', position, '(size=', remaining, ')'
        promise = Q.nfcall(fs.write, fd, data, offset, remaining, position)
        return promise.then((written, _buffer) ->
            if written < remaining
                return this.writeAllData(fd, data, offset + written, position + written)
        )

    writeRootAndHeader:(dataChunks, newRootTree) ->
        rootBuffer = new Buffer(JSON.stringify(newRootTree))
        dataChunks.push(rootBuffer)
        newHeader = _.clone(@header)
        newHeader.ptr += newHeader.sz
        newHeader.sz = rootBuffer.length
        headerBuffer = new Buffer(JSON.stringify(newHeader))
        this.appendAll(@dataFd, Buffer.concat(dataChunks), @header.ptr + @header.sz).then(=>
            this.appendAll(@headerFd, headerBuffer, @headerSz)
        ).then(=>
            @rootTree = newRootTree
            @header = newHeader
            @headerSz += headerBuffer.length
        )
    
    findTerm:(term) ->
        rec = @rootTree[_.sortedIndex(@rootTree, {t:term}, (r) -> r.t) - 1]
        readBuffer(@dataFd, rec.p, rec.s).then((buf) =>
            query = new Stack(@dataFd, buf)
            console.log ' about to call NEXT'
            query.next().then(->query))

    applyUpdates:(updates) ->
        # updates is a list of {
        #     t[terms]: <string list>,
        #     d[ocId]: <int, negative for new ones>,
        #     o[peration]: <one of: 'i'[nsert],'r'[emove],'c'[heck]>
        # }
        console.log 'root tree ', @rootTree, ' updates ', updates
        for update in updates
            if not update.d?
                if update.o != 'i'
                    throw new Error('Object without an ID can only be inserted: '+update)
                @header.m += 1
                update.d = @header.m
        handled_indexes = new Array(@rootTree.length)
        rt = (_.clone(rec) for rec in @rootTree) # copy root tree
        promises = []
        for update in updates
            console.log 'apply consider ', update
            update_terms = update.t
            continue unless update_terms
            for term in update_terms
                idx = _.sortedIndex(rt, {t:term}, (r) -> r.t) - 1
                if handled_indexes[idx]
                    continue
                handled_indexes[idx] = true
                start_term = rt[idx].t
                if idx + 1 == rt.length
                    filterfn = (t) -> start_term <= t
                else
                    end_term = rt[idx + 1].t
                    filterfn = (t) -> start_term <= t < end_term
                subupdates = []
                term_buffer = []
                for update in updates
                    terms = update.t
                    for term in terms
                        if filterfn(term)
                            term_buffer.push(term)
                    if term_buffer
                        subupdates.push({t:term_buffer, d:update.d, o:update.o})
                        update.t = if term_buffer.length == terms.length then null else _.reject(update.t, filterfn)
                        term_buffer = []
                console.log 'sub updates ', subupdates
                promise = readBuffer(@dataFd, rt[idx].p, rt[idx].s).then((buffer) =>
                    items = new BufferStackFrame(buffer).readAll()
                    console.log 'apply to items', subupdates, items
                    this.applyUpdatesToItems(subupdates, items).then(-> [idx, items] )
                )
                promises.push(promise)
        dataChunks = []
        Q.all(promises).then( (buffers) =>
            for [idx, items] in buffers
                ref = this.writeItems(items, dataChunks)
                rt[idx].p = ref.ptr
                rt[idx].s = ref.sz
                rt[idx].c = ref.count
            buffers = null # might give the GC some space?
            return this.writeRootAndHeader(dataChunks, rt)
        )

    writeItems:(items, dataChunks) ->
        count = 0
        console.log 'writeitems ', items
        for item in items
            count += 1
            if _.isArray(item.children)
                item.children = this.writeItems(item.children, dataChunks)
                count += item.children.count
            console.log 'write items ? ', item.children
        ptr = dataChunks.reduce ((prev, cur) -> prev + cur.length), @header.ptr + @header.sz
        chunk = new BufferStackFrame(new Buffer(2048)).writeAllItems(items)
        console.log 'CHUNK ! ', chunk
        dataChunks.push(chunk)
        return {ptr: ptr, sz: chunk.length, count: count}

    spliceUpdates:(updates, items, item_idx) ->
        recs = ({
            docId: update.d,
            terms: update.t,
            count: 1
        } for update in updates)
        console.log 'splice ', recs, ' at position: ', item_idx
        items[item_idx...item_idx] = recs
        
    applyUpdatesToItems:(updates, items) ->
        # when resulting promise is fulfilled, frame items have been updated
        if items.length == 0
            this.spliceUpdates(updates, items, 0)
            return Q.fcall(-> null)
        promises = []
        update_idx = 0
        for item_idx in [0 .. items.length - 1]
            item = items[item_idx]
            curDoc = item.d
            if updates[update_idx].d < curDoc
                console.log 'applyUpdatesToItems found insertion for=',updates[update_idx],' before=', item 
                # make subupdates
                subupdates = [updates[update_idx]]
                update_idx += 1
                while updates[update_idx].d < curDoc
                    update = updates[update_idx]
                    switch update.o
                        when 'c'
                            throw new Exception('concurrent modification')
                        when 'i'
                            subupdates.push(update)
                    update_idx += 1
                children = item.children
                if children is null
                    this.spliceUpdates(subupdates, items, item_idx)
                    item_idx += 1
                else if _.isArray(children)
                    ret = applyUpdatesToFrame(subupdates, children)
                    if ret
                        promises.push(ret)
                else
                    promises.push(
                        readBuffer(@dataFd, children.p, children.s)
                        .then((buffer)=>
                            subitems = new BufferStackFrame(buffer).readAll()
                            applyUpdatesToFrame(subupdates, subitems))
                        .then(->
                            item.children = subitems)
                    )
            if updates[update_idx].d == curDoc
                update = updates[update_idx]
                switch update.o
                    when 'c'
                        if not contains_all(item.t, update.t)
                            throw new Exception('concurrent modification')
                    when 'i'
                        item.t = _.uniq(_.sortBy(_.union(item.t, update.t), _.identity), true)
                    when 'r'
                        item.t = _.difference(item.t, update.t)
        if update_idx < updates.length
            this.spliceUpdates(updates[update_idx..], items, items.length)
            return Q.fcall(-> null)
        return if promises then Q.all(promises) else null
                    
 
readVint = (rec) ->
    num = rec.buffer.readInt32BE(rec.ptr)
    rec.ptr += 4
    return num

readVint = (buffer, ptr) ->
    buffer.readInt32BE(ptr)


#    balance: (list, index) ->
# -- based on node size (versus expected node size based on recursive count)
# expected node size = total recursive list count / (8 @ minimum, 24 @ maximum)
# if node is too small, create new header with its contents plus the contents of
# smallest adjacent and re-evaluate.  if node is too big, split into two equal parts
# and re-evaluate both parts
# After a list is balanced, loop through children and commit any pending nodes
# (but do not yet commit this list itself)
#
#
# advance(int docId) -> int | int promise
#   Advances pointer to current document or later that is greater than or equal to docId.
#   Yields the current document id
# next() -> int | int promise
#   Advances pointer to the next document and return the document id
# check(int docId) -> boolean | boolean promise
#   Checks whether docId is in the list at the current position or later.
#   Pointer is left in an semi-unpositioned state -- it is guaranteed to return any items after
#   docId, but may return some of the items prior
# doc() -> int
#   Returns -1 if next() or advance() has not been called.
#   Returns Number.MAX_VALUE if the query has been completed
#   Otherwise, it is the current document ID
# term() -> Buffer
# termCount() -> int
# getTerm(int index) -> Buffer
#

class BaseQuery
    next: -> this.advance(this.doc() + 1)
    check: (docId) -> this.advance(docId)
    term: () -> this.getTerm(0)
    terms:() ->
        termCt = this.termCount()
        a = new Array(termCt)
        i = 0
        for i in [0..termCt - 1]
            a[i] = this.getTerm(i)
        return a

class PositionalBuffer
    constructor: (@buffer, @ptr) ->
        if not @buffer?
            @buffer = new Buffer(512)
        if not @ptr?
            @ptr = 0
    ensureRoom:(sz) ->
        if @ptr + sz >= @buffer.length
            newbuf = new Buffer(@buffer.length * 2 + sz)
            @buffer.copy(newbuf, 0, 0, @buffer.length)
            @buffer = newbuf
    writeBuffer:(other) ->
        len = other.length
        this.ensureRoom(len)
        other.copy(@buffer, 0, 0, len)
        @ptr += len
    writeString:(other) ->
        # TODO what is the fastest way to calculate this?
        # Buffer.byteLength()? or multiply by 4 for worst case unicode scenario?
        len = Buffer.byteLength(other)
        this.ensureRoom(len)
        numWritten = @buffer.write(other, @ptr, len)
        @ptr += numWritten
    readString:(numBytes) ->
        start = @ptr
        @ptr += numBytes
        return @buffer.toString('utf8', start, @ptr)
    writeBit:(val) ->
        this.ensureRoom(1)
        @buffer.writeUInt8((if val then 1 else 0), @ptr)
        @ptr += 1
    readBit: () ->
        num = @buffer.readUInt8(@ptr)
        @ptr += 1
        return num
    writeVint:(val) ->
        this.ensureRoom(8)
        @buffer.writeUInt32BE(val, @ptr)
        @ptr += 4
    readVint:() ->
        num = @buffer.readUInt32BE(@ptr)
        @ptr += 4
        return num
    encodeAtomic:(val) ->
        switch typeof val
            when 'null'
                @buffer.writeUInt8(_NULL, @ptr)
                @ptr += 1
            when 'boolean'
                @buffer.writeUInt8((if val then _TRUE else _FALSE), @ptr)
                @ptr += 1
            when 'number'
                @buffer.writeUInt8(_NUMBER, @ptr)
                @ptr += 1
                @buffer.writeDoubleBE(val, @ptr)
                encodeNumber(@buffer, @ptr)
                @ptr += 8
            when 'string'
                @buffer.writeUInt8(_STRING, @ptr)
                @ptr += 1
                @ptr += @buffer.write(val, @ptr)
    decodeAtomic: ->
        buf = @buffer
        ptr = @ptr
        switch buf.readUInt8(ptr)
            when _NULL then null
            when _FALSE then false
            when _NUMBER
                ptr += 1
                decodeNumber(buf, ptr)
                num = buf.readDoubleBE(ptr)
                encodeNumber(buf, ptr)
                return num
            when _STRING
                return buf.toString('utf8', ptr + 1)
            when _TRUE then true
    copy: ->
        result = new Buffer(@buffer.length)
        @buffer.copy(result, 0, 0, @ptr)
        copy = new PositionalBuffer(result)
        copy.ptr = @ptr
        return copy
    done: ->
        result = new Buffer(@ptr)
        @buffer.copy(result, 0, 0, @ptr)
        return result

class BufferStackFrame extends PositionalBuffer
    constructor: (buffer) ->
        super(buffer)
        @docId = -1
    readAll: () ->
        items = []
        console.log 'readall', @docId, Number.MAX_VALUE
        if @docId == -1
            this.readEntry()
        while @docId < Number.MAX_VALUE
            item = {
                docId: @docId,
                terms: this.terms(),
                children: undefined
            }
            if @hasChildren
                item['children'] = {'ptr':@childrenPtr, 'sz':@childrenSz}
            items.push(item)
            this.readEntry()
        return items
    writeAllItems: (items) ->
        @ptr = 0
        for item in items
            this.writeEntry(item.docId, item.terms, item.children)
        return @buffer.slice(0, @ptr)
    writeEntry:(docId, terms, childFrame) ->
        console.log 'writeEntry start @ ', @ptr, 
        this.writeBit(childFrame)
        this.writeVint(docId - @docId)
        if childFrame
            this.writeVint(childFrame.ptr)
            this.writeVint(childFrame.sz)
        this.writeVint(terms.length)
        console.log 'writeEntry midpoint @ ', @ptr
        for term in terms
            termlen = term.length
            this.writeVint(termlen)
            this.ensureRoom(termlen)
            term.copy(@buffer, @ptr, 0, termlen)
            @ptr += termlen
        console.log 'writeEntry end @ ', @ptr
    readEntry: () ->
        console.log 'readEntry prev mid @ ', @ptr
        if @ptr != 0
            # skip the term data of current document
            for i in [0..@termCt - 1]
                termlen = this.readVint()
                @ptr += termlen
        if @ptr == @buffer.length
            @docId = Number.MAX_VALUE
            @termCt = 0
            return
        console.log 'readEntry new start @ ', @ptr
        @hasChildren = this.readBit()
        @docId += this.readVint()
        if @hasChildren
            @childrenPtr = this.readVint()
            @childrenSz = this.readVint()
        @termCt = this.readVint()
        console.log 'readEntry got id:', @docId, ' termct:', @termCt, ' children:', @childrenPtr
        console.log 'X', @buffer.slice((if @ptr - 20 < 0 then 0 else @ptr - 20), @ptr)
        console.log 'readEntry new mid @ ', @ptr, @buffer.slice(@ptr)
    doc: () -> @docId
    advance: (docId) ->
        while @docId < docId
            if @ptr == @buffer.length
                return Number.MAX_VALUE
            this.readEntry()
        return @docId
    termCount: () -> @termCt
    getTerm: (i) ->
        originalPtr = @ptr
        while i > 0
            @ptr += this.readVint()
            i -= 1
        termlen = this.readVint()
        buf = new Buffer(termlen)
        @buffer.copy(buf, 0, @ptr, @ptr + termlen)
        @ptr = originalPtr
        return buf
    terms:() ->
        termCt = @termCt
        a = new Array(termCt)
        originalPtr = @ptr
        i = 0
        while i < termCt
            termlen = this.readVint()
            buf = new Buffer(termlen)
            @buffer.copy(buf, 0, @ptr, @ptr + termlen)
            @ptr += termlen
            a[i] = buf
            i += 1
        @ptr = originalPtr
        console.log 'Buffer stack frame terms = ', a, @termCt
        return a

class Stack extends BaseQuery
    constructor: (@dataFd, @buffer) ->
        @stack = [] # stack items are BufferStackFrame objects
        console.log 'new stack starting with ', @buffer
        @curFrame = new BufferStackFrame(@buffer)
    advance:(docId) ->
        console.log 'advance ', docId, @curFrame
        if not @curFrame?
            return Number.MAX_VALUE
        Q.when(@curFrame.advance(docId)).then((id) =>
            console.log 'frame advance = ', id
            if id != Number.MAX_VALUE
                if @curFrame.hasChildren
                    @stack.push(@curFrame)
                    buf = Buffer(@curFrame.childrenSz)
                    promise = Q.nfcall(fs.read, @dataFd, buf, 0,
                        @curFrame.childrenSz,
                        @curFrame.childrenPtr)
                    return promise.then((err, bytesRead) ->
                        @curFrame = new BufferStackFrame(buf)
                        this.advance(docId)
                    )
                else
                    return id
            else
                @curFrame = @stack.pop()
                return this.advance(docId)
        )
    doc:() -> if @curFrame? then @curFrame.doc() else Number.MAX_VALUE
    term:() -> @curFrame.term()
    termCount:() -> @curFrame.termCount()
    getTerm:(idx) -> @curFrame.getTerm(idx)
    terms:() -> @curFrame.terms()

class WrappingQuery
    constructor: (@query) ->
    next: -> @query.next()
    advance: (docId) -> @query.advance(docId)
    check: (docId) -> @query.check(docId)
    term: () -> @query.term()
    terms:() -> @query.terms()
    doc: () -> @query.doc()
    termCount: () -> @query.termCount()
    getTerm: (idx) -> @query.getTerm(idx)

class PromiseOnlyQuery extends WrappingQuery
    constructor: (@query) ->
    next: -> Q.when(@query.next())
    advance: (docId) -> Q.when(@query.advance(docId))
    check: (docId) -> Q.when(@query.check(docId))

class LiteralQuery extends BaseQuery
    constructor: (@list) ->
        @idx = -1
    advance: (docId) ->
        if @idx == -1
            @idx = 0
        len = @list.length
        while @idx < len
            item = @list[@idx]
            curDocId = item.docId
            if curDocId >= docId
                #if curDocId > docId and item.children then:
                #    item.children.advance(docId).then((subDocId) ->
                #        if docId > subDocId then
                #    if (_.isArray(item.children)) then
                #else:
                return Q.fcall(->curDocId)
            @idx += 1
        return Q.fcall(->Number.MAX_VALUE)
    doc: () -> switch @idx
        when @list.length then Number.MAX_VALUE
        when -1 then -1
        else @list[@idx].docId
    termCount: () -> @list[@idx].terms.length
    getTerm: (i) -> @list[@idx].terms[i]

class NotQuery extends BaseQuery
    constructor: (@bag, queries) ->
        @docId = 0
        @subquery = if queries.length == 1 then queries[0] else new OrQuery(@bag, queries)
    advance: (docId) ->
        @queries.advance(docId).then(found =>
            if found and docId == @queries.doc()
                return this.advance(docId + 1)
            @docId = docId
            return true
        )
    doc: () -> @docId
    termCount: () -> 0
    getTerm: (i) -> undefined

class OrQuery extends BaseQuery
    constructor: (@queries) ->
        @docId = -1
    advance: (docId, _idx) ->
        if _idx is undefined
            _idx = 0
            @docId = Number.MAX_VALUE
        else if _idx >= @queries.length
            return @docId
        qu = @queries[_idx]
        qDocId = @queries[_idx].doc()
        if qDocId >= docId
            if qDocId < @docId
                @docId = qDocId
            this.advance docId, _idx + 1
        else
            qu.advance(docId).then((qDocId) => this.advance docId, _idx)

    doc: () -> @docId
    termCount: () ->
        return (q.termCount() for q in @queries when q.doc() == @docId).reduce((a, b) -> a + b)
    getTerm: (i) ->
        docId = @docId
        for query in @queries
            if query.doc() == docId
                curct = query.termCount()
                if i < curct
                    return query.getTerm(i)
                else
                    i -= curct

class InQuery extends OrQuery
    constructor: (@queries) ->
        
class AndQuery extends BaseQuery
    # simply take the smallest size one and always use that as the driver?
    # but size can change over time (with weighting, for example)
    # seems as though additional checks are best performed in the docHead list (assuming
    # it's in memory), most selective first.
    # total size is a good proxy for expense (?)
    constructor: (@queries) ->
    advance: (docId, _idx) ->
        _idx = if _idx is undefined then 0 else _idx
        return docId if _idx >= @queries.length
        qu = @queries[_idx]
        qu.advance(docId).then((qDocId) =>
            if qDocId == docId
                this.advance(docId, _idx + 1)
            else
                this.advance(qDocId, 0))
    doc: () -> @queries[0].doc()
    termCount: () ->
        return (q.termCount() for q in @queries).reduce((a, b) -> a + b)
    getTerm: (i) ->
        for query in @queries
            curct = query.termCount()
            if i < curct
                return query.getTerm(i)
            else
                i -= curct
        
class RangeQuery
    # needs to be updatable at runtime!
    # Two use cases: if next() is going to be called, we want a heap
    # if check() is going to be called, we want an array
    constructor: () ->
        
class ResultsQuery
    constructor: (@criteria, @results) ->
        # returns @results when @criteria exists

class DocHeadFilterQuery
    constructor: (@termRanges) ->
    # Isolates based on range (rounded to term lists)
    # Terms are equally bounded

class AdaptiveQuery
    # lika an AndQuery, but re-evaluates sizes and can change driving query lists?

class DocumentQuery
    constructor: () ->
    # every document, all terms

class FilteredQuery
    construtor: (@query, @accept_fn) ->

class ScoringQuery
    constructor: (@criteria, @rangeEvalfnWeightList) ->


# At the inner level, we construct query objects (AndQuery etc),
# and that's how we can dynamically adjust ranges
# 
# 

#item: (id increment (from previous in this level)), value

#segment: +pointer, +total count

exports.LiteralQuery = LiteralQuery
exports.AndQuery = AndQuery
exports.OrQuery = OrQuery
exports.PrawnIndex = PrawnIndex
exports.termifyObject = termifyObject
exports.encodeNumber = encodeNumber
exports.decodeNumber = decodeNumber
exports.serializeResults = serializeResults
