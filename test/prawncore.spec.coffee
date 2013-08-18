prawn = require('../src/prawncore.coffee')
#require('coffee-trace')
fs = require('fs')
Q = require('Q')

success = (promisefn) ->
    done = false
    error = null
    promise = promisefn().then(
        ->
            done = true
        ,
        (e) ->
            console.log(e.stack)
            error = e
            done = true
        )
    waitsFor(-> done)
    runs(->
        if error
            throw error
        )

checkStdQuery01 = (query) ->
        expect(query.doc()).toEqual(-1)
        query.advance(0).then((docId) ->
          expect(docId, query.doc()).toEqual(1, 1)
          expect(query.termCount()).toEqual(1)
          expect(query.getTerm(0).toString()).toEqual('aa')
          query.next()
        ).then((docId) ->
          expect(docId, query.doc()).toEqual(4, 4)
          expect(query.termCount()).toEqual(2)
          expect(t.toString() for t in query.terms()).toEqual(['bb', 'cc'])
          query.advance(4)
        ).then((docId) ->
          expect(docId, query.doc()).toEqual(4, 4)
          query.next()
        ).then((docId) ->
          expect(docId, query.doc()).toEqual(5, 5)
          expect(query.termCount()).toEqual(0)
          query.advance(8)
        ).then((docId) ->
          expect(docId, query.doc()).toEqual(9, 9)
          expect(query.termCount()).toEqual(1)
          expect(t.toString() for t in query.terms()).toEqual(['a'])
          query.next()
        ).then((docId) ->
          expect(docId, query.doc()).toEqual(Number.MAX_VALUE, Number.MAX_VALUE)
        )

checkStdQuery02 = (query) ->
        query.advance(4).then((docId) ->
          expect(docId, query.doc()).toEqual(4, 4)
          query.check(5)
        ).then((found) ->
          expect(found)
          query.advance(5)
        ).then((docId) ->
          expect(docId, query.doc()).toEqual(5, 5)
          query.check(7)
        ).then((found) ->
          expect(! found)
          query.check(10)
        ).then((found) ->
          expect(! found)
        )

checkStdQueries = (queryFactory) ->
    checkStdQuery01(queryFactory())
    checkStdQuery02(queryFactory())


rec = (docId, terms) -> {docId:docId, terms: (new Buffer(t) for t in terms)}

describe 'Prawn query system', ->

    it 'supports literal queries', ->
        success ->
            checkStdQueries -> new prawn.LiteralQuery([
                rec(1, ['aa']),
                rec(4, ['bb','cc']),
                rec(5, []),
                rec(9, ['a'])
            ])

    it 'supports intersection queries', ->
        success ->
            checkStdQueries -> new prawn.AndQuery([
                new prawn.LiteralQuery([
                    rec(1, ['aa']),
                    rec(4, ['bb']),
                    rec(5, []),
                    rec(6, []),
                    rec(7, []),
                    rec(9, []),
                    rec(12, ['zz'])
                ]),
                new prawn.LiteralQuery([
                    rec(0, []),
                    rec(1, []),
                    rec(4, ['cc']),
                    rec(5, []),
                    rec(9, ['a'])
                ])
            ])

    it 'supports union queries', ->
        success ->
            checkStdQueries -> new prawn.OrQuery([
                new prawn.LiteralQuery([
                    rec(1, ['aa']),
                    rec(4, ['bb']),
                    rec(5, [])
                ]),
                new prawn.LiteralQuery([
                    rec(4, ['cc']),
                    rec(9, ['a'])
                ])
            ])
        
makeTempIndex = ->
    Q.all([
        Q.nfcall(fs.open, '/tmp/prawnidx.header', 'w+'),
        Q.nfcall(fs.open, '/tmp/prawnidx.data', 'w+')
    ]).then( (fds) ->
        [headerFd, dataFd] = fds
        index = new prawn.PrawnIndex({headerFd:headerFd, dataFd:dataFd})
        return index.create().then(-> index.init()).then(-> index)
    )

describe 'Prawn indexes', ->
    index = null
    it 'can be created', ->
        success ->
            makeTempIndex().then((i) ->
                console.log('i', i)
                index = i
            )
            
    it 'encode terms', ->
        terms = prawn.termifyObject('Phil')
        expect(terms.length).toEqual(1)
        len = terms[0].length
        expect(terms[0].slice(len - 4, len).toString()).toEqual('Phil')
        
    it 'encode objects', ->
        terms = prawn.termifyObject({'name':'Phil'})
        expect(terms.length).toEqual(1)
        len = terms[0].length
        expect(terms[0].slice(len - 4, len).toString()).toEqual('Phil')
        
    it 'encode objects with numbers', ->
        terms = prawn.termifyObject({'magic':42})
        expect(terms.length).toEqual(1)
        len = terms[0].length
        prawn.decodeNumber(terms[0], len - 8)
        expect(terms[0].readDoubleBE(len - 8)).toEqual(42)

    it 'can be written to', ->
        success ->
            updates = [ {t:prawn.termifyObject({'id':1, 'name':'Phil'}), d:-1} ]
            console.log index, index.applyUpdates, ' PPPP'
            index.applyUpdates(updates)
            .then(-> console.log 'INDEX1', index)
            .then(-> console.log 'INDEX2', index)
        

  #it 'can make new db files', (done)->
  #  file = 'testdata.prawn'
  #  shrimp = new prawn.ShrimpDb(file)
  #  shrimp.opendb()
  #  shrimp.view().get().then((val) ->
  #    val['x'] = {'foo':1}
  #    val.y = 'my name is Foo!'
  #    shrimp.writeChanges(val)
  #  ).then( ->
  #    expect(fs.statSync(file).size).toBeGreaterThan(20)
  #    fs.unlinkSync(file)
  #  ).done(->done())

