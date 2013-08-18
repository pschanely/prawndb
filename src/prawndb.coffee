fs = require('fs')
util = require('util')
_ = require('underscore')
Q = require('Q')
require('coffee-trace')

# on my 64bit macbook, integers start losing precision around 2^52.
# Need to watch this as it relates to byte offsets in files.

saneTypeOf = (x) ->
  typ = typeof x 
  if typ != 'object'
    typ
  else if Array.isArray(x) 
    'array'
  else if x instanceof ShrimpPromise
    'ShrimpPromise'
  else if x is null then 'null' else 'object'

class ShrimpDb
  constructor: (@filename) ->

  realizePossibleRef: (val) ->
    if val % 1 == 0 # only integers
      realval = Math.floor val / 2
      return if val % 2 == 0 then new ShrimpPromise(this, realval) else realval
    return val

  realize: (struct) ->
    if Array.isArray(struct)
      for val in struct
        this.realize(val)
    else if saneTypeOf struct == 'number'
      return this.realizePossibleRef(struct)
    else
      for key, val of struct
        switch saneTypeOf val
          when 'number'
            struct[key] = this.realizePossibleRef(val)
          when 'object'
            this.realize(val)
    return struct

  readLine: (pos, callback) ->
    buf = new Buffer(4)
    console.log 'about to read', @fd, buf, 0, 2, pos
    fs.read(@fd, buf, 0, 2, pos, (err, bytesRead, buf) =>
      length = buf.readUInt16BE(0, true)
      buf = new Buffer(length)
      fs.read(@fd, buf, 0, length, pos + 2, (err, bytesRead, buf) =>
        result = JSON.parse(buf.toString('ascii'))
        callback(err, this.realize(result))))
  
  writeLine: (obj, stream) ->
    str = JSON.stringify(obj)
    buf = new Buffer(str.length + 2)
    buf.writeUInt16BE(str.length, 0)
    buf.write(str, 2)
    pos = fs.fstatSync(@fd).size
    fs.writeSync(@fd, buf, 0, str.length + 2, pos)
    return pos
   
  opendb: () ->
    if not fs.existsSync(@filename)
      fs.closeSync(fs.openSync(@filename, 'w'))
    @fd = fs.openSync @filename, 'r+'
    if fs.fstatSync(@fd).size == 0
      buf = new Buffer(12)
      buf.writeUInt32BE(10231, 0) # magic
      buf.writeUInt32BE(8, 4) # root is at position 8
      buf.writeUInt16BE(2, 8) # line is 2 bytes long
      buf.write('{}', 10) # the line
      fs.writeSync(@fd, buf, 0, 12, 0)
    buf = new Buffer(12)
    fs.readSync(@fd, buf, 0, 4, 4)
    @rootPointer = buf.readUInt32BE(0)

  newNode: (val) ->
    return new ShrimpPromise(this, undefined, val)

  compareAndWrite: (oldobj, newobj) ->
    console.log 'compareAndWrite', oldobj, newobj
    deferred = Q.defer()
    allSame = true
    switch saneTypeOf(newobj)

      when 'array'
        if not Array.isArray(oldobj)
          allSame = false
          oldobj = []
        result = []
        branches = for pair in _.zip(oldobj, newobj)
          do (pair) =>
            [oldv, newv] = pair
            this.compareAndWrite(oldv, newv).then( (pair) ->
              [item, same] = pair
              result.push(item)
              return same)
        return Q.all(branches).then( (sameFlags) =>
          allSame = allSame and _.every sameFlags
          [result, allSame]
        )
        
      when 'ShrimpPromise'
        if not newobj.realized()
          deferred.resolve([newobj.root,
            oldobj instanceof ShrimpPromise and oldobj.root == newobj.root])
        else
          return Q.when(oldobj, (oldval) =>
            this.compareAndWrite(oldval, newobj.val)
          ).then( (pair) =>
            [item, same] = pair
            return [(if (same and oldobj instanceof ShrimpPromise) then oldobj.root else this.writeLine(item)) * 2, same]
          )
        
      when 'object'
        if saneTypeOf(oldobj) != 'object'
          oldobj = {}
          allSame = false
        branches = for key of newobj
          do (key) =>
            newv = newobj[key]
            this.compareAndWrite(oldobj[key], newv).then( (pair) ->
              [item, same] = pair
              newobj[key] = item
              return same)
        return Q.all(branches).then( (sameFlags) =>
          allSame = allSame and _.every sameFlags
          # one last check to ensure nothing in oldobj was removed
          allSame = allSame and _.every(newobj[key] != undefined for key of oldobj)
          if allSame and oldobj instanceof ShrimpPromise
            newobj = oldobj.root * 2
          return [newobj, allSame]
        )

      when 'number'
        deferred.resolve([newobj * 2 + 1, oldobj == newobj])  
        
      else
        deferred.resolve([newobj, oldobj is newobj])

    return deferred.promise


  writeChanges: (newroot) ->
    this.view().get().then( (oldroot) =>
      this.compareAndWrite(oldroot, newroot)
    ).then( (pair) =>
      [root, same] = pair
      if not same
        if typeof root != 'number'
          root = this.writeLine(root)
        @rootPointer = root
        buf = new Buffer(4)
        buf.writeUInt32BE(root, 0)
        fs.writeSync(@fd, buf, 0, 4, 4)
        fs.fsyncSync(@fd)
      return root
    )

  view: () ->
    new ShrimpView(this, @rootPointer)

class ShrimpView
  constructor: (@db, @rootPointer) ->

  get: (addr) ->
    new ShrimpPromise(@db, if addr is undefined then @rootPointer else addr)

class ShrimpPromise
  constructor: (db, @root, @val) ->
    @deferred = Q.defer()
    if @root isnt undefined
      db.readLine(@root, (err, val) =>
        if err
          @deferred.reject(err)
        else
          @val = val
          @deferred.resolve(val)
      )
  realized: () -> @val isnt undefined
  then: (args...) -> @deferred.promise.then(args...)
  get: () -> @val

exports.ShrimpDb = ShrimpDb
exports.ShrimpPromise = ShrimpPromise
