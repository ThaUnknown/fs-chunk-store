/*! fs-chunk-store. MIT License. Feross Aboukhadijeh <https://feross.org/opensource> */
const fs = require('fs')
const os = require('os')
const parallel = require('run-parallel')
const path = require('path')
const queueMicrotask = require('queue-microtask')
const raf = require('random-access-file')
const randombytes = require('randombytes')
const thunky = require('thunky')

let TMP
try {
  TMP = fs.statSync('/tmp') && '/tmp'
} catch (err) {
  TMP = os.tmpdir()
}

class Storage {
  constructor (chunkLength, opts = {}) {
    this.chunkLength = Number(chunkLength)
    if (!this.chunkLength) throw new Error('First argument must be a chunk length')
    this.path = opts.path

    if (opts.files) {
      if (opts.files.constructor !== Array) throw new Error('`files` option must be an array')
      this.files = opts.files.slice(0).map((file, i, files) => {
        if (file.path == null) throw new Error('File is missing `path` property')
        if (file.length == null) throw new Error('File is missing `length` property')
        if (file.offset == null) {
          if (i === 0) {
            file.offset = 0
          } else {
            const prevFile = files[i - 1]
            file.offset = prevFile.offset + prevFile.length
          }
        }
        if (this.path) file.path = path.resolve(path.join(this.path, file.path))
        return file
      })
      this.length = this.files.reduce((sum, file) => sum + file.length, 0)
      if (opts.length != null && opts.length !== this.length) throw new Error('total `files` length is not equal to explicit `length` option')
    } else {
      const len = Number(opts.length) || Infinity
      if (!this.path) this.path = path.resolve(path.join(TMP, 'fs-chunk-store', randombytes(20).toString('hex')))
      this.files = [{
        offset: 0,
        path: this.path,
        length: len
      }]
      this.length = len
    }

    this.chunkMap = []
    this.closed = false

    this.files.forEach(file => {
      file.open = thunky(cb => {
        if (this.closed) return cb(new Error('Storage is closed'))
        fs.mkdir(path.dirname(file.path), { recursive: true }, err => {
          if (err) return cb(err)
          if (this.closed) return cb(new Error('Storage is closed'))
          cb(null, raf(file.path))
        })
      })
    })

    // If the length is Infinity (i.e. a length was not specified) then the store will
    // automatically grow.

    if (this.length !== Infinity) {
      this.lastChunkLength = (this.length % this.chunkLength) || this.chunkLength
      this.lastChunkIndex = Math.ceil(this.length / this.chunkLength) - 1

      this.files.forEach(file => {
        const fileStart = file.offset
        const fileEnd = file.offset + file.length

        const firstChunk = Math.floor(fileStart / this.chunkLength)
        const lastChunk = Math.floor((fileEnd - 1) / this.chunkLength)

        for (let p = firstChunk; p <= lastChunk; ++p) {
          const chunkStart = p * this.chunkLength
          const chunkEnd = chunkStart + this.chunkLength

          const from = (fileStart < chunkStart) ? 0 : fileStart - chunkStart
          const to = (fileEnd > chunkEnd) ? this.chunkLength : fileEnd - chunkStart
          const offset = (fileStart > chunkStart) ? 0 : chunkStart - fileStart

          if (!this.chunkMap[p]) this.chunkMap[p] = []

          this.chunkMap[p].push({
            from,
            to,
            offset,
            file
          })
        }
      })
    }
  }

  put (index, buf, cb = () => {}) {
    if (this.closed) return nextTick(cb, new Error('Storage is closed'))

    const isLastChunk = (index === this.lastChunkIndex)
    if (isLastChunk && buf.length !== this.lastChunkLength) {
      return nextTick(cb, new Error('Last chunk length must be ' + this.lastChunkLength))
    }
    if (!isLastChunk && buf.length !== this.chunkLength) {
      return nextTick(cb, new Error('Chunk length must be ' + this.chunkLength))
    }

    if (this.length === Infinity) {
      this.files[0].open((err, file) => {
        if (err) return cb(err)
        file.write(index * this.chunkLength, buf, cb)
      })
    } else {
      const targets = this.chunkMap[index]
      if (!targets) return nextTick(cb, new Error('no files matching the request range'))
      const tasks = targets.map(target => {
        return cb => {
          target.file.open((err, file) => {
            if (err) return cb(err)
            file.write(target.offset, buf.slice(target.from, target.to), cb)
          })
        }
      })
      parallel(tasks, cb)
    }
  }

  get (index, opts, cb) {
    if (typeof opts === 'function') return this.get(index, null, opts)
    if (this.closed) return nextTick(cb, new Error('Storage is closed'))

    const chunkLength = (index === this.lastChunkIndex)
      ? this.lastChunkLength
      : this.chunkLength

    const rangeFrom = (opts && opts.offset) || 0
    const rangeTo = (opts && opts.length) ? rangeFrom + opts.length : chunkLength

    if (rangeFrom < 0 || rangeFrom < 0 || rangeTo > chunkLength) {
      return nextTick(cb, new Error('Invalid offset and/or length'))
    }

    if (this.length === Infinity) {
      if (rangeFrom === rangeTo) return nextTick(cb, null, Buffer.from(0))
      this.files[0].open((err, file) => {
        if (err) return cb(err)
        const offset = (index * this.chunkLength) + rangeFrom
        file.read(offset, rangeTo - rangeFrom, cb)
      })
    } else {
      let targets = this.chunkMap[index]
      if (!targets) return nextTick(cb, new Error('no files matching the request range'))
      if (opts) {
        targets = targets.filter(target => {
          return target.to > rangeFrom && target.from < rangeTo
        })
        if (targets.length === 0) {
          return nextTick(cb, new Error('no files matching the requested range'))
        }
      }
      if (rangeFrom === rangeTo) return nextTick(cb, null, Buffer.from(0))

      const tasks = targets.map(target => {
        return cb => {
          let from = target.from
          let to = target.to
          let offset = target.offset

          if (opts) {
            if (to > rangeTo) to = rangeTo
            if (from < rangeFrom) {
              offset += (rangeFrom - from)
              from = rangeFrom
            }
          }

          target.file.open((err, file) => {
            if (err) return cb(err)
            file.read(offset, to - from, cb)
          })
        }
      })

      parallel(tasks, (err, buffers) => {
        if (err) return cb(err)
        cb(null, Buffer.concat(buffers))
      })
    }
  }

  close (cb) {
    if (this.closed) return nextTick(cb, new Error('Storage is closed'))
    this.closed = true

    const tasks = this.files.map(file => {
      return cb => {
        file.open((err, file) => {
          // an open error is okay because that means the file is not open
          if (err) return cb(null)
          file.close(cb)
        })
      }
    })
    parallel(tasks, cb)
  }

  destroy (cb) {
    this.close(() => {
      if (this.path) {
        fs.rm(this.path, { recursive: true, maxRetries: 10 }, err => {
          err && err.code === 'ENOENT' ? cb() : cb(err)
        })
      } else {
        const tasks = this.files.map(file => {
          return cb => {
            fs.rm(file.path, { recursive: true, maxRetries: 10 }, err => {
              err && err.code === 'ENOENT' ? cb() : cb(err)
            })
          }
        })
        parallel(tasks, cb)
      }
    })
  }
}

function nextTick (cb = () => {}, err, val) {
  queueMicrotask(() => cb(err, val))
}

module.exports = Storage
