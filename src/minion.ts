import path from 'path'
import fetch from 'node-fetch'
import { getLayoutVersion, Market } from '@project-serum/serum'
import { Connection, PublicKey } from '@solana/web3.js'
import { Client, DataType } from 'ts-postgres'
import { App, HttpRequest, HttpResponse, DISABLED, SSLApp, TemplatedApp, us_listen_socket_close, WebSocket } from 'uWebSockets.js'
import { isMainThread, threadId, workerData } from 'worker_threads'
import { AnchorProvider, Program, BorshAccountsCoder, EventParser } from '@project-serum/anchor'
import { Metadata, PROGRAM_ID as METAPLEX_PROGRAM_ID } from '@metaplex-foundation/mpl-token-metadata'
import { DateTime } from 'luxon'
import { CHANNELS, MESSAGE_TYPES_PER_CHANNEL, OPS } from './consts'
import {
    cleanupChannel,
    executeAndRetry,
    getAllowedValuesText,
    getDidYouMean,
    marketInitChannel,
    minionReadyChannel,
    aquaDataChannel,
    //aquaMarketsChannel,
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

type EventItem = {
    readonly tx: string
    readonly index: number
    readonly event: string
    readonly data: any
    readonly slot: number
}
type ViewItem = {
    readonly days: number
}

// Minion is the actual HTTP and WS server implementation
// it is meant to run in Node.js worker_thread and handles:
// - HTTP requests
// - WS subscriptions requests
// - WS data publishing to connected clients

class Minion {
    private readonly _server: TemplatedApp | undefined
    private _apiVersion = '1'
    private readonly MAX_MESSAGES_PER_SECOND = 50
    private MAX_BACKPRESSURE = 3 * 1024 * 1024

    // 100 messages per second limit
    private readonly _wsMessagesRateLimit: (ws: any) => boolean = RateLimit(this.MAX_MESSAGES_PER_SECOND, 1000)
    private readonly _l2SnapshotsSerialized: { [market: string]: string } = {}
    private readonly _l3SnapshotsSerialized: { [market: string]: string } = {}
    private readonly _recentTradesSerialized: { [market: string]: string } = {}
    private readonly _quotesSerialized: { [market: string]: string } = {}
    private readonly _marketNames: string[]

    private _sqlClient: Client
    private _aquaProgram: Program
    private _aquaEventParser: EventParser
    private _openConnectionsCount = 0
    private _cachedListMarketsResponse: string | undefined = undefined
    private _cachedMarketInfoResponse: { [market: string]: any } = {}
    private _defaultToken: { [mint: string]: any } = {}
    private _marketBase32: { [market: string]: string } = {}
    private _marketInfo: { [market: string]: any } = {}
    private _listenSocket: any | undefined = undefined
    private _tid: NodeJS.Timeout | undefined = undefined

    mode: 'rpc' | 'api'
    status: AquaMarketStatus
    provider: AnchorProvider

    constructor(private readonly _nodeEndpoint: string, private readonly _markets: AquaMarket[], tokens: any, serviceMode: 'rpc' | 'api', marketStatus: AquaMarketStatus) {
        this.mode = serviceMode
        this.status = marketStatus
        this.provider = AnchorProvider.env()
        this._defaultToken = tokens
        this._marketNames = _markets.map((m) => m.name)
        this._cachedListMarketsResponse = JSON.stringify(_markets)
        this._sqlClient = new Client({
            host: process.env.POSTGRES_HOST,
            port: parseInt(process.env.POSTGRES_PORT as string),
            user: process.env.POSTGRES_USER,
            database: process.env.POSTGRES_DATABASE,
            password: process.env.POSTGRES_PASSWORD,
        })
        const aquaDexPK = new PublicKey(ANCHOR_IDL['aqua-dex'].metadata.address)
        this._aquaProgram = new Program(ANCHOR_IDL['aqua-dex'], aquaDexPK, this.provider)
        this._aquaEventParser = new EventParser(aquaDexPK, this._aquaProgram.coder)
        _markets.forEach((m) => {
            var pk = new PublicKey(m.address)
            var encoder = new base32.Encoder({ type: "crockford", lc: true })
            this._marketBase32[m.address] = 'm' + encoder.write(new Uint8Array(pk.toBuffer().toJSON().data)).finalize()
            this._marketInfo[m.address] = { metadata: {}, ...m }
            delete this._marketInfo[m.address]['nodeEndpoint']
            this._cachedMarketInfoResponse[m.address] = JSON.stringify(this._marketInfo[m.address])
        })
        /*this._tid = setInterval(() => {
            logger.log('debug', `Open WS client connections count: ${this._openConnectionsCount}`, meta)
        }, 60 * 1000)*/
        if (serviceMode === 'api') {
            this._server = this._initServer()
        }
    }

    private _initServer() {
        const apiPrefix = `/v${this._apiVersion}`
        const useSSL = process.env.KEY_FILE_NAME !== undefined
        const WsApp = useSSL ? SSLApp : App
        const options = useSSL ? {
            key_file_name: process.env.KEY_FILE_NAME,
            cert_file_name: process.env.CERT_FILE_NAME
        } : {}
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

            .post(`${apiPrefix}/history`, this._getMarketHistory)
            .options(`${apiPrefix}/history`, (res) => {
                this._setCorsHeaders(res)
                res.end()
            })
            .post(`${apiPrefix}/last_tx`, this._getLastTransaction)
            .options(`${apiPrefix}/last_tx`, (res) => {
                this._setCorsHeaders(res)
                res.end()
            })
            .post(`${apiPrefix}/trades`, this._getTradeList)
            .options(`${apiPrefix}/trades`, (res) => {
                this._setCorsHeaders(res)
                res.end()
            })
            .post(`${apiPrefix}/market_list`, this._getMarketList)
            .options(`${apiPrefix}/market_list`, (res) => {
                this._setCorsHeaders(res)
                res.end()
            })
            .post(`${apiPrefix}/market_info`, this._getMarketInfo)
            .options(`${apiPrefix}/market_info`, (res) => {
                this._setCorsHeaders(res)
                res.end()
            })
    }

    public async start(port: number) {
        return new Promise<void>((resolve, reject) => {
            if (this.mode === 'rpc') {
                resolve()
            }
            this._server?.listen(process.env.MINION_HOST as string, port, (socket) => {
                if (socket) {
                    this._listenSocket = socket
                    logger.log('info', `Minion listening on port ${port}`)
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

    private async _programAddress(inputs: Buffer[], program: PublicKey) {
        const addr = await PublicKey.findProgramAddress(inputs, program)
        const res = { 'pubkey': await addr[0].toString(), 'nonce': addr[1] }
        return res
    }

    private async _getTokenMetadata(mintPK: PublicKey) {
        const mint = mintPK.toString()
        if (mint in this._defaultToken) {
            return this._defaultToken[mint]
        }
        const dataAddr = await this._programAddress([
            Buffer.from('metadata'),
            METAPLEX_PROGRAM_ID.toBuffer(),
            mintPK.toBuffer(),
        ], METAPLEX_PROGRAM_ID)
        try {
            const meta = await Metadata.fromAccountAddress(this.provider.connection, new PublicKey(dataAddr.pubkey))
	        var metaJson = JSON.stringify(meta)
            logger.log('debug', `Token metadata for: ${dataAddr.pubkey} result: ${metaJson}`)
            var name = meta.data.name
            var symbol = meta.data.symbol
            var uri = meta.data.uri
            name = name.replace(/\x00/g, '')
            symbol = symbol.replace(/\x00/g, '')
            uri = uri.replace(/\x00/g, '')
            const spec = {
                'name': name,
                'symbol': symbol,
                'uri': uri,
            }
            return spec
        } catch {
            const mint = mintPK.toString()
            if (mint in this._defaultToken) {
                return this._defaultToken[mint]
            }
            return null
        }
    }

    public async getMarketMetadata() {
        await this._markets.map(async (m: AquaMarket) => {
            const marketInfo = this._marketInfo[m.address]
            const marketPK = new PublicKey(m.address)
            const marketData = await this._aquaProgram.account?.market?.fetch(marketPK)
            //logger.log('debug', 'Market Data ' + JSON.stringify(marketData))
            const mktMint = marketData?.mktMint as PublicKey
            const prcMint = marketData?.prcMint as PublicKey
            //logger.log('debug', 'Market Info ' + JSON.stringify(marketInfo))
            var mktTokenMeta = await this._getTokenMetadata(mktMint)
            var prcTokenMeta = await this._getTokenMetadata(prcMint)
            //logger.log('debug', 'Market Token Meta ' + JSON.stringify(mktTokenMeta))
            //logger.log('debug', 'Pricing Token Meta ' + JSON.stringify(prcTokenMeta))
            marketInfo.metadata.marketToken = {}
            marketInfo.metadata.pricingToken = {}
            if (mktTokenMeta) {
                if (mktTokenMeta.uri) {
                    try {
                        const resp = await fetch(mktTokenMeta.uri)
                        const tdata = await resp.json()
                        mktTokenMeta = { ...mktTokenMeta, ...tdata }
                    } catch {}
                }
                marketInfo.metadata.marketToken = mktTokenMeta
            }
            if (prcTokenMeta) {
                if (prcTokenMeta.uri) {
                    try {
                        const resp = await fetch(prcTokenMeta.uri)
                        const tdata = await resp.json()
                        prcTokenMeta = { ...prcTokenMeta, ...tdata }
                    } catch {}
                }
                marketInfo.metadata.pricingToken = prcTokenMeta
            }
            marketInfo.metadata.marketToken.mint = mktMint.toString()
            marketInfo.metadata.pricingToken.mint = prcMint.toString()
            this._cachedMarketInfoResponse[m.address] = JSON.stringify(marketInfo)
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
        const marketViews: { [view: string]: ViewItem } = {
            'v1m': { days: 1 },
            'v3m': { days: 1 },
            'v5m': { days: 1 },
            'v15m': { days: 2 },
            'v30m': { days: 2 },
            'v1h': { days: 3 },
            'v2h': { days: 3 },
            'v4h': { days: 3 },
            'v6h': { days: 10 },
            'v8h': { days: 10 },
            'v12h': { days: 30 },
            'v1d': { days: 90 },
            'v7d': { days: 365 },
            'v30d': { days: 1095 },
        }
        res.onAborted(() => {
            res.aborted = true
        })
        var rdata: any = await this._getData(res)
        let rq = []
        if ('market' in rdata && 'view' in rdata) {
            try {
                const marketPK = new PublicKey(rdata.market)
                const market = marketPK.toString()
                const view = rdata.view
                if (!market || !this._marketBase32[market] || typeof marketViews[view] === 'undefined') {
                    return this._abortRequest(res, 404)
                }
                logger.log('debug', `Get Market History: market:${market} view:${view}`)
                const viewDuration = marketViews[view]?.days
                const rangeStart = DateTime.now().toUTC().minus({ days: viewDuration }).startOf('day').toJSDate()
                const rangeEnd = DateTime.now().toUTC().endOf('day').toJSDate()
                const historyTable = this._marketBase32[market] + '_' + view
                const historyQuery = await this._sqlClient.query(
                    "SELECT bucket, open, high, low, close FROM " + historyTable + " WHERE bucket >= $1 AND bucket <= $2 ORDER BY bucket DESC",
                    [
                        rangeStart,
                        rangeEnd,
                    ], [
                        DataType.Timestamptz,
                        DataType.Timestamptz,
                    ]
                )
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
            } catch (error) {
                logger.log('error', error)
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
        }
    }

    private _getLastSignature = async (marketInput: string) => {
        var rq = null
        try {
            const marketPK = new PublicKey(marketInput)
            const market = marketPK.toString()
            //logger.log('debug', `Get Latest Signature: market:${market}`)
            const sigQuery = await this._sqlClient.query(
                "SELECT st.sig FROM solana_transaction st, solana_market_transaction smt WHERE smt.market_id=solana_account_id($1) AND smt.transaction_id=st.id ORDER BY smt.transaction_id DESC LIMIT 1",
                [
                    market,
                ], [
                    DataType.Varchar,
                ]
            )
            if (sigQuery && sigQuery.rows.length) {
                rq = sigQuery.rows[0]?.[0]
            }
        } catch (error) {
            logger.log('error', error)
        }
        return rq
    }

    private _getLastTransaction = async (res: HttpResponse) => {
        res.onAborted(() => {
            res.aborted = true
        })
        var rdata: any = await this._getData(res)
        let rq = null
        if ('market' in rdata && 'user' in rdata) {
            try {
                const marketPK = new PublicKey(rdata.market)
                const userPK = new PublicKey(rdata.user)
                const market = marketPK.toString()
                const user = userPK.toString()
                if (!market || !this._marketBase32[market]) {
                    return this._abortRequest(res, 404)
                }
                logger.log('debug', `Get Latest Transaction: market:${market} user:${user}`)
                const txQuery = await this._sqlClient.query(
                    "SELECT st.sig FROM solana_market_ref smr, solana_account sa, solana_transaction st WHERE st.id=smr.transaction_id AND sa.id=smr.user_id AND sa.address=$1 AND smr.event_name='MatchEvent' AND smr.market_id = solana_account_id($2) ORDER BY smr.ts DESC LIMIT 1",
                    [
                        user,
                        market,
                    ], [
                        DataType.Varchar,
                        DataType.Varchar,
                    ]
                )
                if (txQuery && txQuery.rows.length) {
                    rq = txQuery.rows[0]?.[0]
                }
            } catch (error) {
                logger.log('error', error)
            }
        } else {
            logger.log('debug', 'Invalid last transaction request')
            return this._abortRequest(res, 404)
        }
        if (!res.aborted) {
            res.writeStatus('200 OK')
            res.writeHeader('Content-Type', 'application/json')
            this._setCorsHeaders(res)
            res.end(JSON.stringify({
                'result': 'ok',
                'lastTx': rq,
            }))
        }
    }

    private _getTradeList = async (res: HttpResponse) => {
        res.onAborted(() => {
            res.aborted = true
        })
        var rdata: any = await this._getData(res)
        var rq = []
        var ct = Number(0)
        var cts = ''
        if ('market' in rdata && 'user' in rdata) {
            try {
                const marketPK = new PublicKey(rdata.market)
                const userPK = new PublicKey(rdata.user)
                const market = marketPK.toString()
                const user = userPK.toString()
                if (!market || !this._marketBase32[market]) {
                    return this._abortRequest(res, 404)
                }
                logger.log('debug', `Get Trade List: market:${market} user:${user}`)
                const countQuery = await this._sqlClient.query(
                    "SELECT COUNT(smr.id) FROM solana_market_ref smr, solana_account sa, solana_transaction st WHERE st.id=smr.transaction_id AND sa.id=smr.user_id AND sa.address=$1 AND smr.market_id = solana_account_id($2) AND smr.event_name='MatchEvent'",
                    [
                        user,
                        market,
                    ], [
                        DataType.Varchar,
                        DataType.Varchar,
                    ]
                )
                if (countQuery) {
                    ct = countQuery.rows[0]?.[0] as number
                    cts = ct.toString()
                }

                var limit = 10
                var offset = 0
                if ('page' in rdata) {
                    var pg = rdata.page
                    if (!Number.isInteger(pg) ) {
                        return this._abortRequest(res, 404)
                    }
                    var page = new Number(pg)
                    if (page < 1) {
                        return this._abortRequest(res, 404)
                    }
                    offset = limit * (page as number - 1)
                }
                const tradeQuery = await this._sqlClient.query(
                    "SELECT smr.ts, st.sig, (sme.event_data->>'tradeId')::integer as trade_id, sme.event_data, smr.data_key FROM solana_market_ref smr, solana_account sa, solana_transaction st, solana_market_event sme WHERE st.id=smr.transaction_id AND sa.id=smr.user_id AND sa.address=$1 AND smr.market_id = solana_account_id($2) AND smr.event_name='MatchEvent' AND smr.transaction_id=sme.transaction_id AND smr.log_index = sme.log_index ORDER BY trade_id DESC LIMIT $3 OFFSET $4",
                    [
                        user,
                        market,
                        limit,
                        offset,
                    ], [
                        DataType.Varchar,
                        DataType.Varchar,
                        DataType.Int4,
                        DataType.Int4,
                    ]
                )
                if (tradeQuery) {
                    for await (const r of tradeQuery.rows) {
                        rq.push({
                            'ts': r[0],
                            'sig': r[1],
                            'data': r[3],
                            'role': r[4],
                        })
                    }
                }
            } catch (error) {
                logger.log('error', error)
            }
        } else {
            logger.log('debug', 'Invalid last transaction request')
            return this._abortRequest(res, 404)
        }
        if (!res.aborted) {
            res.writeStatus('200 OK')
            res.writeHeader('Content-Type', 'application/json')
            this._setCorsHeaders(res)
            res.end(JSON.stringify({
                'result': 'ok',
                'count': new Number(cts),
                'trades': rq,
            }))
        }
    }


    //async based on https://github.com/uNetworking/uWebSockets.js/blob/master/examples/AsyncFunction.js
    private _getMarketList = async (res: HttpResponse) => {
        res.onAborted(() => {
            res.aborted = true
        })
        if (!res.aborted) {
            res.writeStatus('200 OK')
            res.writeHeader('Content-Type', 'application/json')
            this._setCorsHeaders(res)
            res.end(this._cachedListMarketsResponse)
        }
    }

    private _getMarketInfo = async (res: HttpResponse) => {
        res.onAborted(() => {
            res.aborted = true
        })
        var resp = null
        var rdata: any = await this._getData(res)
        if ('market' in rdata) {
            resp = this._cachedMarketInfoResponse[rdata.market]
            logger.log('debug', `Get Market Info: market:${rdata.market}`)
        } else {
            logger.log('debug', 'Market parameter not found')
            return this._abortRequest(res, 404)
        }
        if (!res.aborted) {
            res.writeStatus('200 OK')
            res.writeHeader('Content-Type', 'application/json')
            this._setCorsHeaders(res)
            res.end(resp)
        }
    }

    private async _storeSignatures(signatures: string[], slots: { [tx: string]: number }, market: string) {
        signatures.reverse()
        for (const sig of signatures) {
            try {
                if (sig.length > 0) {
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
            } catch (error) {
                logger.log('error', `Store Sig: ${sig} Error: ${error}`)
            }
        }
    }

    private async _storeEventRef(event: EventItem, market: string, program: string, dkey: string, user: string) {
        try {
            await this._sqlClient.query(
                "INSERT INTO solana_market_ref (transaction_id, log_index, market_id, program_id, user_id, data_key, event_name) VALUES (solana_tx_id($1), $2, solana_account_id($3), solana_account_id($4), solana_account_id($5), $6, $7) ON CONFLICT DO NOTHING",
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
        } catch (error) {
            logger.log('error', `Store Event Ref: ${event.tx}-${event.index}-${dkey} Error: ${error}`)
        }
    }

    private async _storeEvents(events: EventItem[], market: string, program: string) {
        events.reverse()
        for (const event of events) {
            try {
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
            } catch (error) {
                logger.log('error', `Store Event: ${event.tx}-${event.index} Error: ${error}`)
            }
            if (event.event === 'OrderEvent') {
                await this._storeEventRef(event, market, program, 'user', event.data.user)
            } else if (event.event === 'SettleEvent') {
                await this._storeEventRef(event, market, program, 'owner', event.data.owner)
            } else if (event.event === 'MatchEvent') {
                await this._storeEventRef(event, market, program, 'maker', event.data.maker)
                await this._storeEventRef(event, market, program, 'taker', event.data.taker)
            } else if (event.event === 'WithdrawEvent') {
                await this._storeEventRef(event, market, program, 'user', event.data.user)
                await this._storeEventRef(event, market, program, 'owner', event.data.owner)
            } else if (event.event === 'VaultWithdrawEvent') {
                await this._storeEventRef(event, market, program, 'user', event.data.user)
                await this._storeEventRef(event, market, program, 'owner', event.data.owner)
            } else if (event.event === 'VaultDepositEvent') {
                await this._storeEventRef(event, market, program, 'owner', event.data.owner)
            }
        }
    }

    public processMessages(messages: MessageEnvelope[]) {
        for (const message of messages) {
            const topic = `${message.type}-${message.market}`

            if (logger.level === 'debug') {
                //const diff = new Date().valueOf() - new Date(message.timestamp).valueOf()
                const data = JSON.stringify(message.payload)
                logger.log('debug', `Processing message: topic:${topic} data:${data}`, meta)
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
                this._getLastSignature(message.market).then((lastSig) => {
                    var sigOpts: any = {
                        limit: 200,
                    }
                    if (lastSig) {
                        sigOpts['until'] = lastSig
                        logger.log('debug', `Get Signatures: ${message.payload.market} Until: ${lastSig}`)
                    } else {
                        logger.log('debug', `Get Signatures: ${message.payload.market}`)
                    }
                    this.provider.connection.getSignaturesForAddress(new PublicKey(message.payload.market), sigOpts, 'confirmed').then((csi: any) => {
                        setTimeout(function(csi: any, thiz: Minion) {
                            var txlist: string[] = []
                            for (const item of csi) {
                                txlist.push(item.signature)
                            }
                            thiz.provider.connection.getTransactions(txlist, {commitment: 'confirmed', maxSupportedTransactionVersion: 0}).then((trl) => {
                                const program = thiz._aquaProgram.programId.toString()
                                var eventList: EventItem[] = []
                                var slots: { [tx: string]: number } = {}
                                for (var i = 0; i < trl.length; i++) {
                                    logger.log('debug', `Sig: ${txlist[i]}`)
                                    if (trl[i]) {
                                        if (trl[i]?.meta?.err) {
                                            logger.log('debug', `Sig: ${txlist[i]} Error`)
                                            txlist[i] = ''
                                            continue
                                        }
                                        const slot = trl[i]?.slot
                                        slots[txlist[i] as string] = slot as number
                                        const logs: string[] = trl[i]?.meta?.logMessages as string[]
                                        const sdata = JSON.stringify(logs, null, 4)
                                        //logger.log('debug', `Sig Data: ${sdata}`)
                                        const events = [...thiz._aquaEventParser.parseLogs(logs)]
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
                                thiz._storeSignatures(txlist, slots, message.market).then(() => {
                                    logger.log('debug', `Stored Signatures: ${txlist.length}`)
                                })
                                thiz._storeEvents(eventList, message.market, program).then(() => {
                                    //const evdata = JSON.stringify(eventList, null, 4)
                                    //logger.log('debug', `Stored Events: ${evdata}`)
                                    logger.log('debug', `Stored Events: ${eventList.length}`)
                                })
                            })
                        }, 2000, csi, this)
                    })
                })
            }
            if (message.publish) {
                this._server?.publish(topic, message.payload)
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

const { port, tokens, serviceMode, nodeEndpoint, markets, minionNumber, marketStatus } = workerData as {
    port: number
    tokens: any
    serviceMode: 'rpc' | 'api'
    nodeEndpoint: string
    markets: AquaMarket[]
    minionNumber: number
    marketStatus: AquaMarketStatus
}

const minion = new Minion(nodeEndpoint, markets, tokens, serviceMode, marketStatus)

let lastPublishTimestamp = new Date()

if (minionNumber === 0 && serviceMode === 'rpc') {
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
    if (minion.mode === 'rpc') {
        await minion.createMarkets()
        aquaDataChannel.onmessage = (message) => {
            lastPublishTimestamp = new Date()
            minion.processMessages(message.data)
        }
        marketInitChannel.onmessage = async () => {
            await minion.initMarkets()
        }
    }
    minionReadyChannel.postMessage('ready')
    if (minion.mode === 'api') {
        await minion.getMarketMetadata()
    }
})

cleanupChannel.onmessage = async () => {
    await minion.stop()
}
