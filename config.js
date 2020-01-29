const fs = require('fs')
const yargs = require('yargs')

const argv = yargs
  .command('config', 'Specifies path/filename to json with configuration', {
    config: {
      description: 'the year to check for',
      alias: 'c',
      type: 'string',
    }
  })
  .help()
  .alias('help', 'h')
  .argv

const configJson = fs.readFileSync(argv.config ? argv.config : './config.json')
const config = JSON.parse(configJson.toString())

module.exports = config
