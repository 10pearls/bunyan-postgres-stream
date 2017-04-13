const Writable = require('stream').Writable
const pg = require('pg');
const _ = require('lodash');

class LogStream extends Writable {
  constructor (options) {
    super(options)

    if (options.connection === undefined || options.tableName === undefined) {
      throw new Error('Invalid bunyan-postgres stream configuration')
    }

    if (options.connection.client && options.connection.client.makeKnex) {
      this.knex = options.connection
      this._write = this._writeKnex
    }

    if (typeof options.connection === 'object') {
      this.connection = options.connection
      this._write = this._writePgPool
      this.pool = new pg.Pool(this.connection)
    }

    this.tableName = options.tableName;
    this.schema = options.schema;
    console.log(this.schema);
  }

  _writeKnex (chunk, env, cb) {
    const content = JSON.parse(chunk.toString());
    this.knex.insert(this._logColumns(content))
      .into(this.tableName)
      .asCallback(cb)
  }

  _logColumns(content) {
    if(this.schema){
      let schemaObject = {};
      let columnName = Object.keys(this.schema);
      columnName.forEach(column => {
        schemaObject[column] = _.get(content, this.schema[column], null)
      });

      return schemaObject
    }
    else{
      return {
        name: content.name,
        level: content.level,
        hostname: content.hostname,
        msg: content.msg,
        pid: content.pid,
        time: content.time,
        content: JSON.stringify(content)
      }
    }
  }

  _generateRawQuery(content) {
    let query = `insert into ${this.tableName} (`;
    let columnNames = Object.keys(this.schema);
    columnNames.forEach(column => {
      query = `${query}${column},`
    });
    query = `${query.slice(0, -1)}) values (`;
    columnNames.forEach(column => {
      query = `${query} ${_.get(content, this.schema[column], null)},`
    });
    query = `${query.slice(0, -1)} )`;

    return query;
  }


  writePgPool (client, content) {
    if(this.schema){
      return client.query(
        this._generateRawQuery(content))
    }
    return client.query(`
      insert into ${this.tableName}
        (name, level, hostname, msg, pid, time, content)
      values (
        '${content.name}',
        '${content.level}',
        '${content.hostname}',
        '${content.msg}',
        '${content.pid}',
        '${content.time}',
        '${JSON.stringify(content)}'
      );
    `)
  }

  _writePgPool (chunk, env, cb) {
    const content = JSON.parse(chunk.toString())
    this.pool.connect().then(client => {
      return this.writePgPool(client, content).then(result => {
        cb(null, result.rows)
        client.release()
      })
    })
      .catch(err => cb(err))
  }

  end (cb) {
    if (this.pool) {
      return this.pool.end(cb)
    }
    cb()
  }
}

module.exports = (options = {}) => {
  return new LogStream(options)
};

