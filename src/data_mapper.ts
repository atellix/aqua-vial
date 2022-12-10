import { Buffer } from 'buffer'
import { PublicKey } from '@solana/web3.js'
import { Layout, Sequence, struct, nu64, ns64, seq, u16, u8, blob } from '@solana/buffer-layout'
import { BorshAccountsCoder } from '@project-serum/anchor'
import BN from 'bn.js'
import { logger } from './logger'
import { AccountsNotificationPayload } from './rpc_client'
import { MessageEnvelope } from './aqua_producer'
import { aquaStatusChannel } from './helpers'
import { AquaMarket, AquaMarketAccounts, AquaMarketStatus, Trade, DataMessage, AquaMarketUpdateLastId, MarketEvent } from './types'

const base32 = require('base32.js')

// Input
interface LogEntry {
    event_type: any,
    action_id: number,
    trade_id: number,
    maker_order_id: any,
    maker_filled: number,
    maker: any,
    taker: any,
    taker_side: number,
    amount: number,
    price: number,
    ts: number,
}

interface LogSlabVec {
    offset: any,
    trade_count: number,
    entry_max: number,
    logs: LogEntry[]
}

interface TypedPageItem {
    page_index: number,
}

interface TypedPage {
    header_size: number,
    offset_size: number,
    alloc_items: number,
    alloc_pages: TypedPageItem[],
}

interface SlabAlloc {
    top_unused_page: number,
    type_page: TypedPage[],
    pages: any
}

interface LogSpec {
    trade_count: number,
    entry_max: number,
    logs: Trade[],
}

// DataMapper maps tradeLog accounts data to normalized messages
export class DataMapper {
    private _initialized = false
    status: AquaMarketStatus
    coder: BorshAccountsCoder

    constructor(
        private readonly _options: {
            readonly market: AquaMarket
            readonly accounts: AquaMarketAccounts
        },
        accountCoder: BorshAccountsCoder,
        marketStatus: AquaMarketStatus
    ) {
        this.coder = accountCoder
        this.status = marketStatus
        aquaStatusChannel.onmessage = (message) => {
            const msg: AquaMarketUpdateLastId = message.data as unknown as AquaMarketUpdateLastId
            this.status.lastTradeIds[msg.market] = msg.lastId
            logger.log('debug', `Status channel message: ${msg.market} ${msg.lastId}`)
        }
        logger.log('debug', `Listening to status channel`)
    }

    public *map({ accountsData, slot }: AccountsNotificationPayload): IterableIterator<MessageEnvelope> {
        // the same timestamp for all messages received in single notification
        const timestamp = new Date().toISOString()
        if (accountsData.tradeLog) {
            const tradeLog = this._decodeTradeLog(accountsData.tradeLog, slot, this._options.market.address)
            const lastId = this.status.lastTradeIds[this._options.market.address]
            logger.log('debug', `Last trade id for market ${this._options.market.address}: ${lastId}`)
            // TODO: check for only new trades
            for (const trade of tradeLog.logs) {
                if (trade.trade_id > (lastId ?? 0)) {
                    this.status.lastTradeIds[this._options.market.address] = trade.trade_id
                    yield this._putInEnvelope(trade, false)
                }
            }
        }
        if (accountsData.marketState) {
            logger.log('debug', 'New market state')
            try {
                const state = this.coder.decode('MarketState', accountsData.marketState)
                const marketEvent: MarketEvent = {
                    type: 'event',
                    timestamp: '',
                    market: this._options.market.address,
                    state: this._options.accounts.marketState,
                    version: 1,
                    slot: slot,
                    action_id: state.actionCounter?.toString(),
                }
                yield this._putInEnvelope(marketEvent, false)
            } catch (error) {
                logger.log('error', error)
            }
        }
    }

    private _encodeOrderId(orderIdBuf: any) {
        var zflist = orderIdBuf.toJSON().data
        var zflen = 16 - zflist.length
        if (zflen > 0) {
            var zfprefix = Array(zflen).fill(0)
            zflist = zfprefix.concat(zflist)
        }
        zflist.reverse()
        var encoder = new base32.Encoder({ type: "crockford", lc: true })
        var res = encoder.write(new Uint8Array(zflist)).finalize()
        return res
    }

    private _decodeTradeLog(data: Buffer, slot: number, market: string) {
        const stTypedPageItem = struct<TypedPageItem>([
            u16('page_index')
        ])
        const stTypedPage = struct<TypedPage>([
            nu64('header_size'),
            nu64('offset_size'),
            nu64('alloc_items'),
            seq(stTypedPageItem, 16, 'alloc_pages'), // TYPE_MAX_PAGES
        ]);
        const stSlabAlloc = struct<SlabAlloc>([
            u16('top_unused_page'),
            seq(stTypedPage, 4, 'type_page'), // TYPE_MAX
            seq(blob(16384), 1, 'pages'), // PAGE_SIZE
        ]);
        var res = stSlabAlloc.decode(data)
        var logVec = res['type_page'][0]
        if (logVec) {
            return this._decodeTradeLogVec(logVec, res['pages'], slot, market)
        } else {
            return {
                trade_count: 0,
                entry_max: 0,
                logs: [],
            }
        }
    }

    private _decodeTradeLogVec(pageTableEntry: TypedPage, pages: any, slot: number, market: string) {
        const headerSize = pageTableEntry['header_size']
        const offsetSize = pageTableEntry['offset_size']
        const stLogEntry = struct<LogEntry>([
            blob(16, 'event_type'),
            nu64('action_id'),
            nu64('trade_id'),
            blob(16, 'maker_order_id'),
            u8('maker_filled'),
            blob(32, 'maker'),
            blob(32, 'taker'),
            u8('taker_side'),
            nu64('amount'),
            nu64('price'),
            ns64('ts'),
        ])
        const instPerPage = Math.floor((16384 - (headerSize + offsetSize)) / stLogEntry.span)
        const stSlabVec = struct<LogSlabVec>([
            blob(offsetSize, 'offset'),
            nu64('trade_count'),
            nu64('entry_max'),
            seq(stLogEntry, instPerPage, 'logs'),
        ])
        var totalPages = Math.floor(pageTableEntry['alloc_items'] / instPerPage)
        if ((pageTableEntry['alloc_items'] % instPerPage) !== 0) {
            totalPages = totalPages + 1
        }
        var vecPages = []
        for (var p = 0; p < totalPages; p++) {
            var pidx = pageTableEntry['alloc_pages'][p]
            if (pidx) {
                vecPages.push(pages[pidx.page_index])
            }
        }
        var logSpec: LogSpec = {
            'trade_count': 0,
            'entry_max': 0,
            'logs': [],
        }
        for (var i = 0; i < vecPages.length; i++) {
            var res = stSlabVec.decode(vecPages[i])
            if (i === 0) {
                logSpec['trade_count'] = res['trade_count']
                logSpec['entry_max'] = res['entry_max']
            }
            for (var logIdx = 0; logIdx < res['logs'].length; logIdx++) {
                var item = res['logs'][logIdx]
                if (item) {
                    if (item['trade_id'] === 0) {
                        continue
                    }
                    var trade: Trade = {
                        type: 'trade',
                        timestamp: item['ts'].toString(),
                        market: market,
                        version: 1,
                        slot: slot,
                        event_type: (new BN(item['event_type'])).toString(),
                        action_id: item['action_id'],
                        trade_id: item['trade_id'],
                        maker_order_id: this._encodeOrderId(item['maker_order_id']),
                        maker_filled: item['maker_filled'],
                        maker: new PublicKey(item['maker']).toString(),
                        taker: new PublicKey(item['taker']).toString(),
                        taker_side: item['taker_side'],
                        amount: item['amount'],
                        price: item['price'],
                    }
                    logSpec['logs'].push(trade)
                    if (logSpec['logs'].length === pageTableEntry['alloc_items']) {
                        i = vecPages.length
                        break
                    }
                }
            }
        }
        logSpec.logs = logSpec.logs.sort((a, b) => { return a.trade_id - b.trade_id })
        return logSpec
    }

    private _putInEnvelope(message: DataMessage, publish: boolean) {
        const envelope: MessageEnvelope = {
            type: message.type,
            market: message.market,
            publish,
            payload: message,
            timestamp: message.timestamp
        }
        return envelope
    }
}

