import { Buffer } from 'buffer'
import { PublicKey } from '@solana/web3.js'
import { Layout, Sequence, struct, nu64, ns64, seq, u16, u8, blob } from '@solana/buffer-layout'
import BN from 'bn.js'
import { logger } from './logger'
import { AccountsNotificationPayload } from './rpc_client'
import { MessageEnvelope } from './aqua_producer'
import { AquaMarket, AquaMarketAccounts } from './types'

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

interface LogItem {
    event_type: string,
    trade_id: number,
    maker: string,
    taker: string,
}

interface LogSpec {
    trade_count: number,
    entry_max: number,
    logs: LogItem[],
}

// DataMapper maps tradeLog accounts data to normalized messages
export class DataMapper {
    private _initialized = false

    constructor(
        private readonly _options: {
            readonly market: AquaMarket
            readonly accounts: AquaMarketAccounts
        }
    ) { }

    public *map({ accountsData }: AccountsNotificationPayload): IterableIterator<MessageEnvelope> {
        // the same timestamp for all messages received in single notification
        const timestamp = new Date().toISOString()
        if (accountsData.tradeLog) {
            const tradeLog = JSON.stringify(this._decodeTradeLog(accountsData.tradeLog))
            logger.log('info', `${tradeLog}`)
        }
        //// yield this._putInEnvelope(recentTradesMessage, false)
    }

    private _decodeTradeLog(data: Buffer) {
        const stTypedPageItem = struct<TypedPageItem>([
            u16('page_index')
        ])
        const stTypedPage = struct<TypedPage>([
            nu64('header_size'),
            nu64('offset_size'),
            nu64('alloc_items'),
            seq(stTypedPageItem, 128, 'alloc_pages'), // TYPE_MAX_PAGES
        ]);
        const stSlabAlloc = struct<SlabAlloc>([
            u16('top_unused_page'),
            seq(stTypedPage, 16, 'type_page'), // TYPE_MAX
            seq(blob(16384), 4, 'pages'), // PAGE_SIZE
        ]);
        var res = stSlabAlloc.decode(data)
        var logVec = res['type_page'][0]
        if (logVec) {
            return this._decodeTradeLogVec(logVec, res['pages'])
        }
        return []
    }

    private _decodeTradeLogVec(pageTableEntry: TypedPage, pages: any) {
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
                    //item['maker_order_id'] = this.encodeOrderId(item['maker_order_id'])
                    var logItem: LogItem = {
                        event_type: (new BN(item['event_type'])).toString(),
                        trade_id: item['trade_id'],
                        maker: new PublicKey(item['maker']).toString(),
                        taker: new PublicKey(item['taker']).toString()
                    }
                    logSpec['logs'].push(logItem)
                    if (logSpec['logs'].length === pageTableEntry['alloc_items']) {
                        i = vecPages.length
                        break
                    }
                }
            }
        }
        logSpec.logs = logSpec.logs.sort((a, b) => { return b.trade_id - a.trade_id })
        return logSpec
    }
}

