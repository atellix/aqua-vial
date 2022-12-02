import { PublicKey } from '@solana/web3.js'
import { isMainThread, workerData } from 'worker_threads'
import { MessageType } from './consts'
import { DataMapper } from './data_mapper'
import { decimalPlaces, aquaDataChannel, aquaProducerReadyChannel, aquaStatusChannel, marketInitChannel } from './helpers'
import { logger } from './logger'
import { RPCClient } from './rpc_client'
import { AquaMarket, AquaMarketAccounts, AquaMarketStatus } from './types'

if (isMainThread) {
    const message = 'Exiting. Worker is not meant to run in main thread'
    logger.log('error', message)

    throw new Error(message)
}

process.on('unhandledRejection', (err) => {
    throw err
})

// AquaProducer responsibility is to:
// - connect to Aqua Node RPC API via WS and subscribe to single Aqua market
// - map received data to normalized data messages and broadcast those

export class AquaProducer {
    status: AquaMarketStatus

    constructor(
        private readonly _options: {
            nodeEndpoint: string
            wsEndpointPort: number | undefined
            market: AquaMarket
            commitment: string
        }
    ) {
        this.status = {
            lastTradeIds: {}
        }
    }

    public async run(onData: OnDataCallback) {
        let started = false
        logger.log('info', `Aqua producer starting for ${this._options.market.name} market...`)

        // TODO: read this
        const accounts = {
            tradeLog: 'Czb7W4cRVtGdbMUk19zSh3bEPJg9fPkM56MmuXW6phjU'
        }

        // don't use Solana web3.js Connection but custom rpcClient so we have more control and insight what is going on
        const rpcClient = new RPCClient({
            nodeEndpoint: this._options.nodeEndpoint,
            commitment: this._options.commitment,
            wsEndpointPort: this._options.wsEndpointPort
        })
    
        const dataMapper = new DataMapper({
            market: this._options.market,
            accounts
        }, this.status)

        let start = process.hrtime()
        const interval = 600

        // based on https://github.com/tj/node-blocked/blob/master/index.js
        setInterval(() => {
            const delta = process.hrtime(start)
            const nanosec = delta[0] * 1e9 + delta[1]
            const ms = nanosec / 1e6
            const n = ms - interval

            if (n > 200) {
                logger.log('info', `Event loop blocked for ${Math.round(n)} ms.`, {
                    market: this._options.market.name
                })
            }

            start = process.hrtime()
        }, interval).unref()

        if (started === false) {
            logger.log('info', `Aqua producer started for ${this._options.market.name} market...`)
            started = true
            aquaProducerReadyChannel.postMessage('ready')
        }

        marketInitChannel.onmessage = async () => {
            for await (const notification of rpcClient.streamAccountsNotification(accounts, this._options.market.name)) {
                const messagesForSlot = [...dataMapper.map(notification)]

                if (messagesForSlot.length > 0) {
                    onData(messagesForSlot)
                }
            }
        }
    }
}

const aquaProducer = new AquaProducer(workerData)

aquaProducer.run((envelopes) => {
    aquaDataChannel.postMessage(envelopes)
})

export type MessageEnvelope = {
    type: MessageType
    market: string
    publish: boolean
    payload: any
    timestamp: string
}

type OnDataCallback = (envelopes: MessageEnvelope[]) => void
