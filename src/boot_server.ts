import os from 'os'
import path from 'path'
import { Worker } from 'worker_threads'
import { cleanupChannel, minionReadyChannel, aquaProducerReadyChannel, marketInitChannel, wait } from './helpers'
import { logger } from './logger'
import { AquaMarket, AquaMarketStatus } from './types'

export async function bootServer({
    port,
    tokens,
    markets,
    serviceMode,
    nodeEndpoint,
    wsEndpointPort,
    minionsCount,
    commitment,
    bootDelay
}: BootOptions) {
    // multi core support is linux only feature which allows multiple threads to bind to the same port
    // see https://github.com/uNetworking/uWebSockets.js/issues/304 and https://lwn.net/Articles/542629/
    const MINIONS_COUNT = os.platform() === 'linux' ? minionsCount : 1
    let readyMinionsCount = 0
    let marketStatus: AquaMarketStatus = {
        lastTradeIds: {}
    }

    logger.log(
        'info',
        MINIONS_COUNT === 1 ? 'Starting single minion worker...' : `Starting ${MINIONS_COUNT} minion workers...`
    )
    minionReadyChannel.onmessage = () => readyMinionsCount++

    // start minions workers and wait until all are ready

    for (let i = 0; i < MINIONS_COUNT; i++) {
        const minionWorker = new Worker(path.resolve(__dirname, 'minion.js'), {
            workerData: { nodeEndpoint, port, markets, tokens, serviceMode, minionNumber: i, marketStatus }
        })
        minionWorker.on('error', (err) => {
            logger.log('error', `Minion worker ${minionWorker.threadId} error occurred: ${err.message} ${err.stack}`)
            throw err
        })
        minionWorker.on('exit', (code) => {
            logger.log('error', `Minion worker: ${minionWorker.threadId} died with code: ${code}`)
        })
    }

    await new Promise<void>(async (resolve) => {
        while (true) {
            if (readyMinionsCount === MINIONS_COUNT) {
                break
            }
            await wait(100)
        }

        resolve()
    })

    if (serviceMode === 'rpc') {
        logger.log('info', `Starting aqua producers for ${markets.length} markets, rpc endpoint: ${nodeEndpoint}`)
        let readyProducersCount = 0
        aquaProducerReadyChannel.onmessage = () => readyProducersCount++
        for (const market of markets) {
            const aquaProducerWorker = new Worker(path.resolve(__dirname, 'aqua_producer.js'), {
                workerData: { market, nodeEndpoint, commitment, wsEndpointPort }
            })

            aquaProducerWorker.on('error', (err) => {
                logger.log(
                    'error',
                    `Aqua producer worker ${aquaProducerWorker.threadId} error occurred: ${err.message} ${err.stack}`
                )
                throw err
            })

            aquaProducerWorker.on('exit', (code) => {
                logger.log('error', `Aqua producer worker: ${aquaProducerWorker.threadId} died with code: ${code}`)
            })

            // just in case to not get hit by aqua RPC node rate limits...
            await wait(bootDelay)
        }

        await new Promise<void>(async (resolve) => {
            while (true) {
                if (readyProducersCount === markets.length) {
                    break
                }
                await wait(100)
            }

            resolve()
        })
    }

    marketInitChannel.postMessage('ready')
}

export async function stopServer() {
    cleanupChannel.postMessage('cleanup')

    await wait(10 * 1000)
}

type BootOptions = {
    port: number
    tokens: any
    markets: AquaMarket[]
    serviceMode: 'rpc' | 'api'
    nodeEndpoint: string
    wsEndpointPort: number | undefined
    minionsCount: number
    commitment: string
    bootDelay: number
}
