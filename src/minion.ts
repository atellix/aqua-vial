import path from 'path'
import { getLayoutVersion, Market } from '@project-serum/serum'
import { Connection, PublicKey } from '@solana/web3.js'
import { Client, DataType } from 'ts-postgres'
import { App, HttpRequest, HttpResponse, DISABLED, SSLApp, TemplatedApp, us_listen_socket_close, WebSocket } from 'uWebSockets.js'
import { isMainThread, threadId, workerData } from 'worker_threads'
import { AnchorProvider, Program, BorshAccountsCoder, EventParser } from '@project-serum/anchor'
import { CHANNELS, MESSAGE_TYPES_PER_CHANNEL, OPS } from './consts'
import {
    cleanupChannel,
    executeAndRetry,
    getAllowedValuesText,
    getDidYouMean,
    marketInitChannel,
    minionReadyChannel,
    aquaDataChannel,
    aquaMarketsChannel,
    aquaStatusChannel,
    wait
} from './helpers'
import { logger } from './logger'
import { MessageEnvelope } from './aqua_producer'
import { ErrorResponse, RecentTrades, AquaMarket, AquaMarketStatus, SubRequest, SuccessResponse, AquaMarketUpdateLastId, MarketHistoryQuery } from './types'

const base32 = require('base32.js')

const ANCHOR_IDL = {
    'aqua-dex': require(path.join(process.cwd(), 'idl/aqua_dex.json')),
}

const meta = {
    minionId: threadId
}

if (isMainThread) {
    const message = 'Exiting. Worker is not meant to run in main thread'
    logger.log('error', message, meta)

    throw new Error(message)
}

process.on('unhandledRejection', (err) => {
    throw err
})

// based on https://github.com/uNetworking/uWebSockets.js/issues/335#issuecomment-643500581
const RateLimit = (limit: number, interval: number) => {
    let now = 0
    const last = Symbol(),
        count = Symbol()
    setInterval(() => ++now, interval)
    return (ws: any) => {
        if (ws[last] != now) {
            ws[last] = now
            ws[count] = 1

            return false
        } else {
            return ++ws[count] > limit
        }
    }
}

//type TPromiseObjectNull = Promise<{ [key: string]: any } | null>

type EventItem = {
    readonly tx: string
    readonly index: number
    readonly event: string
    readonly data: any
    readonly slot: number
}

// Minion is the actual HTTP and WS server implementation
// it is meant to run in Node.js worker_thread and handles:
// - HTTP requests
// - WS subscriptions requests
// - WS data publishing to connected clients

class Minion {
    private readonly _server: TemplatedApp
    private _apiVersion = '1'
    private readonly MAX_MESSAGES_PER_SECOND = 50

    // 100 messages per second limit
    private readonly _wsMessagesRateLimit: (ws: any) => boolean = RateLimit(this.MAX_MESSAGES_PER_SECOND, 1000)

    private _sqlClient: Client

    private readonly _l2SnapshotsSerialized: { [market: string]: string } = {}
    private readonly _l3SnapshotsSerialized: { [market: string]: string } = {}
    private readonly _recentTradesSerialized: { [market: string]: string } = {}
    private readonly _quotesSerialized: { [market: string]: string } = {}
    private readonly _marketNames: string[]
    private _marketBase32: { [market: string]: string } = {}
    private _listenSocket: any | undefined = undefined
    private _openConnectionsCount = 0
    private _tid: NodeJS.Timeout | undefined = undefined
    private _aquaProgram: Program
    private _aquaEventParser: EventParser

    status: AquaMarketStatus
    provider: AnchorProvider

    private MAX_BACKPRESSURE = 3 * 1024 * 1024
    constructor(private readonly _nodeEndpoint: string, private readonly _markets: AquaMarket[], marketStatus: AquaMarketStatus) {
        this.provider = AnchorProvider.env()
        this.status = marketStatus
        const aquaDexPK = new PublicKey(ANCHOR_IDL['aqua-dex'].metadata.address)
        this._aquaProgram = new Program(ANCHOR_IDL['aqua-dex'], aquaDexPK, this.provider)
        this._aquaEventParser = new EventParser(aquaDexPK, this._aquaProgram.coder)
        this._marketNames = _markets.map((m) => m.name)
        _markets.forEach((m) => {
            var pk = new PublicKey(m.address)
            var encoder = new base32.Encoder({ type: "crockford", lc: true })
            this._marketBase32[m.address] = 'm' + encoder.write(new Uint8Array(pk.toBuffer().toJSON().data)).finalize()
        })
        this._server = this._initServer()
        this._sqlClient = new Client({
            host: process.env.POSTGRES_HOST,
            port: parseInt(process.env.POSTGRES_PORT as string),
            user: process.env.POSTGRES_USER,
            database: process.env.POSTGRES_DATABASE,
            password: process.env.POSTGRES_PASSWORD,
        })

        this._tid = setInterval(() => {
            logger.log('debug', `Open WS client connections count: ${this._openConnectionsCount}`, meta)
        }, 60 * 1000)

    }

    private _initServer() {
        const apiPrefix = `/v${this._apiVersion}`
        const useSSL = process.env.KEY_FILE_NAME !== undefined
        const WsApp = useSSL ? SSLApp : App
        const options = useSSL
            ? {
                    key_file_name: process.env.KEY_FILE_NAME,
                    cert_file_name: process.env.CERT_FILE_NAME
                }
            : {}
        return WsApp(options)
            // Websocket disabled
            /*.ws(`${apiPrefix}/ws`, {
                compression: DISABLED,
                maxPayloadLength: 256 * 1024,
                idleTimeout: 60, // closes WS connection if no message/ping send/received in 1 minute
                maxBackpressure: this.MAX_BACKPRESSURE, // close if client is too slow to read the data fast enough
                closeOnBackpressureLimit: true,
                message: (ws: any, message: any) => {
                    this._handleSubscriptionRequest(ws, message)
                },
                open: () => {
                    this._openConnectionsCount++
                },
                close: () => {
                    this._openConnectionsCount--
                }
            } as any)*/

            //.get(`${apiPrefix}/markets`, this._listMarkets) // TODO
            .post(`${apiPrefix}/history`, this._getMarketHistory)
            .options(`${apiPrefix}/history`, (res) => {
                this._setCorsHeaders(res)
                res.end()
            })
    }

    public async start(port: number) {
        return new Promise<void>((resolve, reject) => {
            this._server.listen(process.env.MINION_HOST as string, port, (socket) => {
                if (socket) {
                    this._listenSocket = socket
                    logger.log('info', `Listening on port ${port}`, meta)
                    resolve()
                } else {
                    const message = `Failed to listen on port ${port}`
                    logger.log('error', message, meta)
                    reject(new Error(message))
                }
            })
        })
    }

    public async stop() {
        if (this._listenSocket !== undefined) {
            us_listen_socket_close(this._listenSocket)
        }

        if (this._tid !== undefined) {
            clearInterval(this._tid)
        }
    }

    public async sqlConnect() {
        await this._sqlClient.connect()
        logger.log('info', `Connected to Postgres/TimescaleDB server`)
    }

    public async tableExists(table: string) {
        const tableQuery = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '" + table + "');"
        var res = await this._sqlClient.query(tableQuery)
        var found = res.rows[0]?.[0]
        return found
    }

    public async createMarketTables(m: AquaMarket, baseName: string) {
        logger.log('debug', `Build market tables for: ${m.name} (${m.address})`)
        const tableTrades = `CREATE TABLE ${baseName}_trades (
trade_id BIGINT NOT NULL,
action_id BIGINT NOT NULL,
match_type CHAR(32),
maker_order_id CHAR(32),
maker_filled BOOL,
maker_id BIGINT NOT NULL,
taker_id BIGINT NOT NULL,
taker_side BOOL NOT NULL,
ts TIMESTAMPTZ NOT NULL,
quantity BIGINT NOT NULL,
price BIGINT NOT NULL);`
        const tableTradesIndex = `CREATE UNIQUE INDEX ${baseName}_ix1 ON ${baseName}_trades(trade_id, ts);`
        const tableTradesHT = `SELECT * FROM create_hypertable('${baseName}_trades', 'ts');`
        const intervals = [ // table_postfix, bucket, start_offset, end_offset, schedule_interval
            ['v1m', '1 min', '2 hour', '10 sec', '2 min'],
            ['v3m', '3 min', '2 hour', '10 sec', '6 min'],
            ['v5m', '5 min', '2 hour', '10 sec', '10 min'],
            ['v15m', '15 min', '2 hour', '10 sec', '30 min'],
            ['v30m', '30 min', '2 hour', '10 sec', '1 hour'],
            ['v1h', '60 min', '3 hour', '10 sec', '2 hour'],
            ['v2h', '120 min', '6 hour', '10 sec', '4 hour'],
            ['v4h', '240 min', '12 hour', '10 sec', '8 hour'],
            ['v6h', '360 min', '18 hour', '10 sec', '12 hour'],
            ['v8h', '480 min', '24 hour', '10 sec', '16 hour'],
            ['v12h', '720 min', '36 hour', '10 sec', '24 hour'],
            ['v1d', '1 day', '72 hour', '10 sec', '48 hour'],
            ['v7d', '7 day', '21 day', '10 sec', '14 day'],
            ['v30d', '30 day', '90 day', '10 sec', '60 day'],
        ]
        if (! await this.tableExists(baseName + '_trades')) {
            logger.log('debug', `Create market tables for: ${m.name} (${m.address})`)
            await this._sqlClient.query(tableTrades)
            await this._sqlClient.query(tableTradesIndex)
            await this._sqlClient.query(tableTradesHT)
            for (var j = 0; j < intervals.length; j++) {
                const i = intervals[j]
                const tablePostfix = i?.[0]
                const bucket = i?.[1]
                const startOffset = i?.[2]
                const endOffset = i?.[3]
                const schedInterval = i?.[4]
                const viewCandles = `CREATE MATERIALIZED VIEW ${baseName}_${tablePostfix}
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('${bucket}', ts) AS bucket,
    FIRST(price, trade_id) AS "open",
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price, trade_id) AS "close",
    SUM(quantity) AS volume
FROM ${baseName}_trades
GROUP BY bucket
ORDER BY bucket DESC;`
                const viewCandlesRefresh = `SELECT add_continuous_aggregate_policy('${baseName}_${tablePostfix}',
start_offset => INTERVAL '${startOffset}',
end_offset => INTERVAL '${endOffset}',
schedule_interval => INTERVAL '${schedInterval}');`
                await this._sqlClient.query(viewCandles)
                await this._sqlClient.query(viewCandlesRefresh)
                logger.log('debug', `Create view for: ${baseName}_${tablePostfix})`)
            }
        }
    }

    public async createMarkets() {
        await this._markets.map(async (m: AquaMarket) => {
            const baseName = this._marketBase32[m.address]
            await this.createMarketTables(m, this._marketBase32[m.address] as string)
        })
    }

    public async initMarkets() {
        await this._markets.map(async (m: AquaMarket) => {
            const baseName = this._marketBase32[m.address]
            const mxrs = await this._sqlClient.query('SELECT MAX(trade_id) AS ct FROM ' + baseName + '_trades;')
            const mx = mxrs.rows[0]?.[0]
            logger.log('debug', `Set last trade id for market ${m.address}: ${mx}`)
            var lastId: number = 0
            if (mx) {
                lastId = mx as number
            }
            this.status.lastTradeIds[m.address] = lastId
            const lastTradeId: AquaMarketUpdateLastId = {
                type: 'lastId',
                market: m.address,
                lastId: lastId,
            }
            aquaStatusChannel.postMessage(lastTradeId)
        })
    }

    private _setCorsHeaders(res: HttpResponse) {
        res.writeHeader("Access-Control-Allow-Origin", "*")
        res.writeHeader("Access-Control-Allow-Methods", "OPTIONS, POST")
        res.writeHeader("Access-Control-Allow-Headers", "origin, content-type, content-length, accept, host, user-agent")
        res.writeHeader("Access-Control-Max-Age", "3600")
    }

    private _abortRequest(res: HttpResponse, status: number) {
        if (!res.aborted) {
            if (status === 404) {
                res.writeStatus('404 Not Found')
            } else {
                res.writeStatus('500 Internal Error')
            }
            this._setCorsHeaders(res)
            res.end()
        }
    }

    private async _getData(res: HttpResponse) {
        const pr = new Promise((resolve) => {
            let buffer: Buffer
            res.onData((ab, isLast) => {
                const chunk = Buffer.from(ab)
                if (isLast) {
                    const toParse = buffer ? Buffer.concat([buffer, chunk]) : chunk
                    const resolveValue = JSON.parse(toParse as unknown as string)
                    resolve(resolveValue)
                } else {
                    const concatValue = buffer ? [buffer, chunk] : [chunk]
                    buffer = Buffer.concat(concatValue)
                }
            })
        })
        var result
        try {
            result = await pr
        } catch (error) {
            logger.log('error', `Reading JSON failed: ${error}`)
        }
        return result
    }

    private _getMarketHistory = async (res: HttpResponse) => {
        const marketViews: { [view: string]: boolean } = {
            'v1m': true,
            'v3m': true,
            'v5m': true,
            'v15m': true,
            'v30m': true,
            'v1h': true,
            'v2h': true,
            'v4h': true,
            'v6h': true,
            'v8h': true,
            'v12h': true,
            'v1d': true,
            'v7d': true,
            'v30d': true,
        }
        res.onAborted(() => {
            res.aborted = true
        })
        var rdata: any = await this._getData(res)
        let rq = []
        if ('market' in rdata && 'view' in rdata) {
            var market = rdata.market
            var view = rdata.view
            if (!market || !this._marketBase32[market] || !marketViews[view]) {
                return this._abortRequest(res, 404)
            }
            logger.log('debug', `Get Market History: ${market}`)
            //const todayStart = new Date(new Date().setHours(0, 0, 0, 0))
            //const todayEnd = new Date(new Date().setHours(23, 59, 59, 999))
            const monthStart = new Date(new Date(new Date().getFullYear(), new Date().getMonth(), 1).setHours(0, 0, 0, 0))
            const monthEnd = new Date(new Date(new Date().getFullYear(), new Date().getMonth() + 1, 0).setHours(23, 59, 59, 999))
            const historyTable = this._marketBase32[market] + '_' + view
            var historyQuery
            try {
                historyQuery = await this._sqlClient.query(
                    "SELECT bucket, open, high, low, close FROM " + historyTable + " WHERE bucket >= $1 AND bucket <= $2 ORDER BY bucket DESC",
                    [
                        monthStart,
                        monthEnd,
                    ], [
                        DataType.Timestamptz,
                        DataType.Timestamptz,
                    ]
                )
            } catch (error) {
                logger.log('error', error)
            }
            if (historyQuery) {
                for await (const r of historyQuery.rows) {
                    rq.push({
                        x: r[0],
                        y: [
                            r[1]?.toString(),
                            r[2]?.toString(),
                            r[3]?.toString(),
                            r[4]?.toString(),
                        ]
                    })
                }
            }
        } else {
            logger.log('debug', 'Invalid market history request')
            return this._abortRequest(res, 404)
        }
        if (!res.aborted) {
            res.writeStatus('200 OK')
            res.writeHeader('Content-Type', 'application/json')
            this._setCorsHeaders(res)
            res.end(JSON.stringify({
                'result': 'ok',
                'history': rq,
            }))
            logger.log('debug', `Sending reply...`)
        }
    }

    private _cachedListMarketsResponse: string | undefined = undefined

    //async based on https://github.com/uNetworking/uWebSockets.js/blob/master/examples/AsyncFunction.js
    private _listMarkets = async (res: HttpResponse) => {
        res.onAborted(() => {
            res.aborted = true
        })

        /*if (this._cachedListMarketsResponse === undefined) {
            const markets = await Promise.all(
                this._markets.map((market) => {
                    return executeAndRetry(
                        async () => {
                            const connection = new Connection(this._nodeEndpoint)
                            const { tickSize, minOrderSize, baseMintAddress, quoteMintAddress, programId } = await Market.load(
                                connection,
                                new PublicKey(market.address),
                                undefined,
                                new PublicKey(market.programId)
                            )

                            const [baseCurrency, quoteCurrency] = market.name.split('/')
                            const serumMarket: SerumListMarketItem = {
                                name: market.name,
                                baseCurrency: baseCurrency!,
                                quoteCurrency: quoteCurrency!,
                                version: getLayoutVersion(programId),
                                address: market.address,
                                programId: market.programId,
                                baseMintAddress: baseMintAddress.toBase58(),
                                quoteMintAddress: quoteMintAddress.toBase58(),
                                tickSize,
                                minOrderSize,
                                deprecated: market.deprecated
                            }
                            return serumMarket
                        },
                        { maxRetries: 10 }
                    )
                })
            )

            this._cachedListMarketsResponse = JSON.stringify(markets, null, 2)
            serumMarketsChannel.postMessage(this._cachedListMarketsResponse)
        }*/

        await wait(1)

        if (!res.aborted) {
            res.writeStatus('200 OK')
            res.writeHeader('Content-Type', 'application/json')
            res.end(this._cachedListMarketsResponse)
        }
    }

    public initMarketsCache(cachedResponse: string) {
        this._cachedListMarketsResponse = cachedResponse
        logger.log('info', 'Cached markets info response', meta)
    }

    private async _storeSignatures(signatures: string[], slots: { [tx: string]: number }, market: string) {
        signatures.reverse()
        for (const sig of signatures) {
            await this._sqlClient.query(
                "INSERT INTO solana_market_transaction (transaction_id, market_id, slot) VALUES (solana_tx_id($1), solana_account_id($2), $3) ON CONFLICT DO NOTHING;",
                [
                    sig,
                    market,
                    slots[sig] as number,
                ], [
                    DataType.Varchar,
                    DataType.Varchar,
                    DataType.Int8,
                ]
            )
        }
    }

    private async _storeEventRef(event: EventItem, market: string, program: string, dkey: string, user: string) {
        await this._sqlClient.query(
            "INSERT INTO solana_market_ref (transaction_id, log_index, market_id, program_id, user_id, data_key, event_name) VALUES (solana_tx_id($1), $2, solana_account_id($3), solana_account_id($4), solana_account_id($5), $6, $7) ON CONFLICT DO NOTHING;",
            [
                event.tx,
                event.index,
                market,
                program,
                user,
                dkey,
                event.event,
            ], [
                DataType.Varchar,
                DataType.Int4,
                DataType.Varchar,
                DataType.Varchar,
                DataType.Varchar,
                DataType.Varchar,
                DataType.Varchar,
            ]
        )
    }

    private async _storeEvents(events: EventItem[], market: string, program: string) {
        events.reverse()
        for (const event of events) {
            await this._sqlClient.query(
                "INSERT INTO solana_market_event (transaction_id, log_index, market_id, slot, program_id, event_name, event_data) VALUES (solana_tx_id($1), $2, solana_account_id($3), $4, solana_account_id($5), $6, $7) ON CONFLICT DO NOTHING;",
                [
                    event.tx,
                    event.index,
                    market,
                    event.slot,
                    program,
                    event.event,
                    event.data,
                ], [
                    DataType.Varchar,
                    DataType.Int4,
                    DataType.Varchar,
                    DataType.Int8,
                    DataType.Varchar,
                    DataType.Varchar,
                    DataType.Json,
                ]
            )
            if (event.event === 'OrderEvent') {
                this._storeEventRef(event, market, program, 'user', event.data.user)
            } else if (event.event === 'SettleEvent') {
                this._storeEventRef(event, market, program, 'owner', event.data.owner)
            } else if (event.event === 'MatchEvent') {
                this._storeEventRef(event, market, program, 'maker', event.data.maker)
                this._storeEventRef(event, market, program, 'taker', event.data.taker)
            }
        }
    }

    public processMessages(messages: MessageEnvelope[]) {
        for (const message of messages) {
            const topic = `${message.type}-${message.market}`

            if (logger.level === 'debug') {
                const diff = new Date().valueOf() - new Date(message.timestamp).valueOf()
                const data = JSON.stringify(message.payload)
                logger.log('debug', `Processing message, topic: ${topic}, receive delay: ${diff}ms, data: ${data}`, meta)
            }
            if (message.type === 'trade') {
                if (message.payload.trade_id > (this.status.lastTradeIds[message.market] as number)) {
                    logger.log('debug', `New trade found... ${message.market}:${message.payload.trade_id}`)
                    const dt = new Date(message.payload.timestamp * 1000)
                    const marketTable = this._marketBase32[message.market] + '_trades'
                    const insert = async () => {
                        try {
                            await this._sqlClient.query(
                                "INSERT INTO " + marketTable + "(trade_id, action_id, maker_order_id, maker_filled, maker_id, taker_id, taker_side, ts, quantity, price) VALUES ($1, $2, $3, $4, solana_account_id($5), solana_account_id($6), $7, $8, $9, $10)",
                                [
                                    message.payload.trade_id,
                                    message.payload.action_id,
                                    message.payload.maker_order_id,
                                    (message.payload.maker_filled === 1) ? true : false,
                                    message.payload.maker,
                                    message.payload.taker,
                                    (message.payload.taker_side === 1) ? true : false,
                                    dt,
                                    message.payload.amount,
                                    message.payload.price,
                                ], [
                                    DataType.Int8,
                                    DataType.Int8,
                                    DataType.Varchar,
                                    DataType.Bool,
                                    DataType.Varchar,
                                    DataType.Varchar,
                                    DataType.Bool,
                                    DataType.Timestamptz,
                                    DataType.Int8,
                                    DataType.Int8,
                                ]
                            )
                        } catch (error) {
                            logger.log('error', error)
                        }
                    }
                    insert()
                }
            }
            if (message.type === 'event') {
                logger.log('debug', `Get Signatures For Address: ${message.payload.state}`)
                this.provider.connection.getSignaturesForAddress(new PublicKey(message.payload.state), {}, 'confirmed').then((csi: any) => {
                    var txlist: string[] = []
                    for (const item of csi) {
                        txlist.push(item.signature)
                    }
                    this.provider.connection.getTransactions(txlist, {commitment: 'confirmed', maxSupportedTransactionVersion: 0}).then((trl) => {
                        const program = this._aquaProgram.programId.toString()
                        var eventList: EventItem[] = []
                        var slots: { [tx: string]: number } = {}
                        for (var i = 0; i < trl.length; i++) {
                            logger.log('debug', `Sig: ${txlist[i]}`)
                            if (trl[i]) {
                                const slot = trl[i]?.slot
                                slots[txlist[i] as string] = slot as number
                                const logs: string[] = trl[i]?.meta?.logMessages as string[]
                                const sdata = JSON.stringify(logs, null, 4)
                                //logger.log('debug', `Sig Data: ${sdata}`)
                                const events = [...this._aquaEventParser.parseLogs(logs)]
                                for (var j = 0; j < events.length; j++) {
                                    const ev = events[j]
                                    if (ev) {
                                        for (const k of Object.keys(ev.data)) {
                                            const evrow = ev.data[k] as any
                                            if (typeof evrow['toString'] === 'function') {
                                                ev.data[k] = evrow.toString()
                                            }
                                        }
                                        const evt: EventItem = {
                                            tx: txlist[i] as string,
                                            index: j,
                                            slot: slot as number,
                                            event: ev.name,
                                            data: ev.data,
                                        }
                                        eventList.push(evt)
                                    }
                                }
                            }
                        }
                        this._storeSignatures(txlist, slots, message.market).then(() => {
                            logger.log('debug', `Stored Signatures`)
                        })
                        this._storeEvents(eventList, message.market, program).then(() => {
                            const evdata = JSON.stringify(eventList, null, 4)
                            logger.log('debug', `Stored Events: ${evdata}`)
                        })
                    })
                })
            }
            if (message.publish) {
                this._server.publish(topic, message.payload)
            }
        }
    }

    private async _handleSubscriptionRequest(ws: WebSocket, buffer: ArrayBuffer) {
        try {
            if (this._wsMessagesRateLimit(ws)) {
                const message = `Too many requests, slow down. Current limit: ${this.MAX_MESSAGES_PER_SECOND} messages per second.`
                logger.log('info', message, meta)

                const errorMessage: ErrorResponse = {
                    type: 'error',
                    message,
                    timestamp: new Date().toISOString()
                }

                await this._send(ws, () => JSON.stringify(errorMessage))

                return
            }

            const message = Buffer.from(buffer).toString()

            if (message === 'ping' || message === 'PING') {
                return
            }

            const validationResult = this._validateRequestPayload(message)

            if (validationResult.isValid === false) {
                logger.log('debug', `Invalid subscription message received, error: ${validationResult.error}`, {
                    message,
                    ...meta
                })

                const errorMessage: ErrorResponse = {
                    type: 'error',
                    message: validationResult.error,
                    timestamp: new Date().toISOString()
                }

                await this._send(ws, () => JSON.stringify(errorMessage))

                return
            }

            const request = validationResult.request

            // 'unpack' channel to specific message types that will be published for it
            const requestedTypes = MESSAGE_TYPES_PER_CHANNEL[request.channel]
            for (const market of request.markets) {
                for (const type of requestedTypes) {
                    const topic = `${type}-${market}`
                    if (request.op === 'subscribe') {
                        if (ws.isSubscribed(topic)) {
                            continue
                        }

                        if (type === 'recent_trades') {
                            const recentTrades = this._recentTradesSerialized[market]
                            if (recentTrades !== undefined) {
                                await this._send(ws, () => this._recentTradesSerialized[market])
                            } else {
                                const emptyRecentTradesMessage: RecentTrades = {
                                    type: 'recent_trades',
                                    market,
                                    timestamp: new Date().toISOString(),
                                    trades: []
                                }

                                await this._send(ws, () => JSON.stringify(emptyRecentTradesMessage))
                            }
                        }

                        if (type === 'quote') {
                            await this._send(ws, () => this._quotesSerialized[market])
                        }

                        if (type == 'l2snapshot') {
                            await this._send(ws, () => this._l2SnapshotsSerialized[market])
                        }

                        if (type === 'l3snapshot') {
                            await this._send(ws, () => this._l3SnapshotsSerialized[market])
                        }

                        const succeeded = ws.subscribe(topic)
                        if (!succeeded) {
                            logger.log('info', `Subscribe failure`, {
                                topic,
                                bufferedAmount: ws.getBufferedAmount()
                            })
                        }
                    } else {
                        if (ws.isSubscribed(topic)) {
                            ws.unsubscribe(topic)
                        }
                    }
                }
            }
            const confirmationMessage: SuccessResponse = {
                type: request.op == 'subscribe' ? 'subscribed' : 'unsubscribed',
                channel: request.channel,
                markets: request.markets,
                timestamp: new Date().toISOString()
            }

            await this._send(ws, () => JSON.stringify(confirmationMessage))

            logger.log('debug', request.op == 'subscribe' ? 'Subscribe successfully' : 'Unsubscribed successfully', {
                successMessage: confirmationMessage,
                ...meta
            })
        } catch (err: any) {
            const message = 'Subscription request internal error'
            const errorMessage = typeof err === 'string' ? err : `${err.message}, ${err.stack}`

            logger.log('info', `${message}, ${errorMessage}`, meta)
            try {
                ws.end(1011, message)
            } catch {}
        }
    }

    private async _send(ws: WebSocket, getMessage: () => string | undefined) {
        let retries = 0
        while (ws.getBufferedAmount() > this.MAX_BACKPRESSURE / 2) {
            await wait(10)
            retries += 1

            if (retries > 200) {
                ws.end(1008, 'Too much backpressure')
                return
            }
        }

        const message = getMessage()
        if (message !== undefined) {
            ws.send(message)
        }
    }

    private _validateRequestPayload(message: string) {
        let payload
        try {
            payload = JSON.parse(message) as SubRequest
        } catch {
            return {
                isValid: false,
                error: `Invalid JSON`
            } as const
        }

        if (OPS.includes(payload.op) === false) {
            return {
                isValid: false,
                error: `Invalid op: '${payload.op}'.${getDidYouMean(payload.op, OPS)} ${getAllowedValuesText(OPS)}`
            } as const
        }

        if (CHANNELS.includes(payload.channel) === false) {
            return {
                isValid: false,
                error: `Invalid channel provided: '${payload.channel}'.${getDidYouMean(
                    payload.channel,
                    CHANNELS
                )}    ${getAllowedValuesText(CHANNELS)}`
            } as const
        }

        if (!Array.isArray(payload.markets) || payload.markets.length === 0) {
            return {
                isValid: false,
                error: `Invalid or empty markets array provided.`
            } as const
        }

        if (payload.markets.length > 100) {
            return {
                isValid: false,
                error: `Too large markets array provided (> 100 items).`
            } as const
        }

        for (const market of payload.markets) {
            if (this._marketNames.includes(market) === false) {
                return {
                    isValid: false,
                    error: `Invalid market name provided: '${market}'.${getDidYouMean(
                        market,
                        this._marketNames
                    )} ${getAllowedValuesText(this._marketNames)}`
                } as const
            }
        }

        return {
            isValid: true,
            error: undefined,
            request: payload
        } as const
    }
}

const { port, nodeEndpoint, markets, minionNumber, marketStatus } = workerData as {
    port: number
    nodeEndpoint: string
    markets: AquaMarket[]
    minionNumber: number
    marketStatus: AquaMarketStatus
}

const minion = new Minion(nodeEndpoint, markets, marketStatus)

let lastPublishTimestamp = new Date()

if (minionNumber === 0) {
    setInterval(() => {
        const noDataPublishedForSeconds = (new Date().valueOf() - lastPublishTimestamp.valueOf()) / 1000
        if (noDataPublishedForSeconds > 30) {
            logger.log('info', `No market data published for prolonged time`, {
                lastPublishTimestamp: lastPublishTimestamp.toISOString(),
                noDataPublishedForSeconds
            })
        }
    }, 15 * 1000).unref()
}

minion.start(port).then(async () => {
    await minion.sqlConnect()
    await minion.createMarkets()

    aquaDataChannel.onmessage = (message) => {
        lastPublishTimestamp = new Date()

        minion.processMessages(message.data)
    }

    aquaMarketsChannel.onmessage = (message) => {
        minion.initMarketsCache(message.data)
    }

    marketInitChannel.onmessage = async () => {
        await minion.initMarkets()
    }

    minionReadyChannel.postMessage('ready')
})

cleanupChannel.onmessage = async () => {
    await minion.stop()
}
