#!/usr/bin/env node

const path = require('path')
const yargs = require('yargs')
const isDocker = require('is-docker')
const pkg = require('../package.json')

const DEFAULT_PORT = 8000
const DEFAULT_NODE_ENDPOINT = 'https://api.devnet.solana.com'

const argv = yargs
    .scriptName('aqua-vial')
    .env('SV_')
    .strict()

    .option('port', {
        type: 'number',
        describe: 'Port to bind server on',
        default: DEFAULT_PORT
    })

    .option('endpoint', {
        type: 'string',
        describe: 'Solana RPC node endpoint that aqua-vial uses as a data source',
        default: DEFAULT_NODE_ENDPOINT
    })

    .option('ws-endpoint-port', {
        type: 'number',
        describe:
            'Optional Solana RPC WS node endpoint port that aqua-vial uses as a data source (if different than REST endpoint port)',
        default: undefined
    })

    .option('log-level', {
        type: 'string',
        describe: 'Log level',
        choices: ['debug', 'info', 'warn', 'error'],
        default: 'info'
    })
    .option('minions-count', {
        type: 'number',
        describe:
            'Minions worker threads count that are responsible for broadcasting normalized WS messages to connected clients',
        default: 1
    })

    .option('commitment', {
        type: 'string',
        describe: 'Solana commitment level to use when communicating with RPC node',
        choices: ['processed', 'confirmed', 'finalized'],
        default: 'confirmed'
    })

    .option('boot-delay', {
        type: 'string',
        describe: 'Staggered boot delay in milliseconds so public RPC nodes do not rate limit aqua-vial',
        default: 500
    })

    .option('markets-json', {
        type: 'string',
        describe: 'Path to custom market.json definition file',
        default: ''
    })

    .option('tokens-json', {
        type: 'string',
        describe: 'Path to tokens.json definition file',
        default: ''
    })

    .option('service-mode', {
        type: 'string',
        describe: 'Service mode: "api" (default) or "rpc"',
        default: 'api'
    })

    .help()
    .version()
    .usage('$0 [options]')
    .example(`$0 --endpoint ${DEFAULT_NODE_ENDPOINT}`)
    .epilogue('See https://github.com/atellix/aqua-vial for more information.')
    .detectLocale(false).argv

// if port ENV is defined use it otherwise use provided options
const port = process.env.PORT ? +process.env.PORT : argv['port']
process.env.LOG_LEVEL = argv['log-level']

const { bootServer, logger } = require('../dist')

async function start() {
    const marketsJsonPath = argv['markets-json']
    if (marketsJsonPath) {
        try {
            const fullPath = path.join(process.cwd(), argv['markets-json'])
            markets = require(fullPath)
            logger.log('info', `Loaded markets from ${fullPath}`)
        } catch (error) {
            logger.log('error', `Required parameter --markets-json=<FILE> not found or invalid JSON file: ${error}`)
            process.exit(1)
        }
    }

    const tokensJsonPath = argv['tokens-json']
    if (tokensJsonPath) {
        try {
            const fullPath = path.join(process.cwd(), argv['tokens-json'])
            tokens = require(fullPath)
            logger.log('info', `Loaded tokens from ${fullPath}`)
        } catch (error) {
            logger.log('error', `Required parameter --tokens-json=<FILE> not found or invalid JSON file: ${error}`)
            process.exit(1)
        }
    }

    const options = {
        port,
        tokens,
        markets,
        serviceMode: argv['service-mode'],
        nodeEndpoint: argv['endpoint'],
        wsEndpointPort: argv['ws-endpoint-port'],
        minionsCount: argv['minions-count'],
        commitment: argv['commitment'],
        bootDelay: argv['boot-delay'],
    }

    //logger.log('info', 'Starting aqua-vial server with options', options)

    const startTimestamp = new Date().valueOf()
    await bootServer(options)
    const bootTimeSeconds = Math.ceil((new Date().valueOf() - startTimestamp) / 1000)

    if (isDocker()) {
        logger.log('info', `Aqua-vial ${options.serviceMode} server v${pkg.version} is running inside Docker container.`, { bootTimeSeconds })
    } else {
        logger.log('info', `Aqua-vial ${options.serviceMode} server v${pkg.version} is running.`, { bootTimeSeconds })
    }

    logger.log('info', `See https://github.com/atellix/aqua-vial for more information.`)
}

start()

process
    .on('unhandledRejection', (reason, p) => {
        console.error('Unhandled Rejection at Promise', reason, p)
        process.exit(1)
    })
    .on('uncaughtException', (err) => {
        console.error('Uncaught Exception thrown', err)
        process.exit(1)
    })
