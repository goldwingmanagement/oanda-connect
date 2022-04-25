/*
Environment           <Domain>
fxTrade               stream-fxtrade.oanda.com
fxTrade Practice      stream-fxpractice.oanda.com
sandbox               stream-sandbox.oanda.com
*/
import { createLogger, format, transports } from 'winston';
import {Db, MongoClient, ObjectId} from 'mongodb';
const { combine, timestamp, printf } = format;

// tslint:disable-next-line:no-shadowed-variable
const myFormat = printf(({ level, message, timestamp }) => {
    return `${timestamp} ${level}: ${message}`;
});

const logger = createLogger({
    format: combine(
        timestamp(),
        myFormat
    ),
    transports: [new transports.Console()]
});

// eslint-disable-next-line @typescript-eslint/no-var-requires
require('dotenv').config();
const exchangeName = 'oanda';
const domain = 'stream-fxtrade.oanda.com';
const accessToken = process.env.APIKEY ? process.env.APIKEY : '';
const accountId = process.env.ACCOUNTID ? process.env.ACCOUNTID : '';
const mongodbUrl = process.env.MONGODBURL ? process.env.MONGODBURL : '';
const mongodbName = process.env.MONGODBNAME ? process.env.MONGODBNAME : '';
const timeframeList = process.env.TIMEFRAMES ? process.env.TIMEFRAMES : '';
const timeframeNameList = process.env.TIMEFRAMENAMES ? process.env.TIMEFRAMENAMES : '';
let db: Db;
let dbConnected = false;
let exchangeId: ObjectId | undefined;

interface HashTable<T> {
    [key: string]: T
}

interface Instrument {
    symbol: string,
    exchange: string,
    marketSymbol: string,
    epoch: number,
    timestamp: Date,
    bid: number,
    ask: number,
    exchangeId: ObjectId | undefined
}

interface Timeframe {
    exchange: string,
    symbol: string,
    marketSymbol: string,
    timeframe: string,
    minutes: number,
    exchangeId: ObjectId | undefined
}

// interface Candlestick {
//     timestamp: Date,
//     symbol: string,
//     open: number,
//     high: number,
//     low: number,
//     close: number,
//     volume: number,
//     exchangeId: string | null
// }

interface Ticker {
    symbol: string,
    epoch: number,
    timestamp: Date,
    bid: number,
    ask: number,
    exchangeId: ObjectId | undefined
}

const Instruments: HashTable<Instrument> = {};
const Timeframes: HashTable<Timeframe> = {};

const timeframes: string[] = timeframeList.split(',');
const timeframeNames: string[] = timeframeNameList.split(',');
const mongoClient = new MongoClient(mongodbUrl);

const instrumentList: string[] = process.env.INSTRUMENTS ? process.env.INSTRUMENTS.split(',') : [];
logger.info('Instruments: ' + instrumentList);
let instruments = '';
instrumentList.map(instrument => {
    const symbol = instrument.replace('_', '/');
    Instruments[symbol] = {
        exchange: exchangeName,
        symbol,
        marketSymbol: instrument,
        exchangeId: undefined,
        epoch: new Date().getTime(),
        timestamp: new Date(),
        bid: 0,
        ask: 0
    }
    // tslint:disable-next-line:prefer-for-of
    for (let i = 0; i < timeframes.length; i++) {
        Timeframes[symbol + '-' + timeframeNames[i]] = {
            timeframe: timeframeNames[i],
            minutes: Number.parseInt(timeframes[i], 10),
            symbol,
            marketSymbol: instrument,
            exchange: exchangeName,
            exchangeId: undefined
        }
    }
    instruments += instrument + '%2C';
});
instruments = instruments.slice(0, instruments.length - 3);

// tslint:disable-next-line:no-var-requires
const https = domain.indexOf('stream-sandbox') > -1 ? require('http') : require('https');
const options = {
    host: domain,
    path: '/v3/accounts/' + accountId + '/pricing/stream?instruments=' + instruments,
    method: 'GET',
    headers: {'Authorization' : 'Bearer ' + accessToken},
};

let heartbeat: Date;

const request = https.request(options, (response: any) => {
    let bodyChunk = '';
    response.on('data', (chunk: any) => {
        bodyChunk += chunk.toString().trim();
        if (bodyChunk.endsWith('}')) {
            const data = bodyChunk;
            bodyChunk = '';
            const parts: string[] = data.split('\n');
            parts.forEach(part => {
                processMessage(part);
            });
        }
    });
    response.on('end', () => {
        logger.error('Error connecting to OANDA HTTP Rates Server');
        logger.error(' - HTTP - ' + response.statusCode);
        // Connection is bad, stop process, if ran in Docker, this should restart the process
        process.exit(1);
    });
});

const processMessage = (message: string) => {
    try {
        if (message === '' || message === null) return;
        const json = JSON.parse(message);
        switch (json.type) {
        case 'HEARTBEAT':
            heartbeat = new Date(json.time);
            ProcessHeartbeat(heartbeat);
            break;
        case 'PRICE':
            // eslint-disable-next-line no-case-declarations
            const ticker: Ticker = {
                symbol: json.instrument.replace('_', '/'),
                epoch: new Date(json.time).getTime(),
                timestamp: new Date(json.time),
                bid: json.closeoutBid,
                ask: json.closeoutAsk,
                exchangeId
            };
            ProcessTicker(ticker);
            break;
        }
    }
    catch (err) {
        logger.error(err);
    }
}

const connect = async () => {
    try {
        await mongoClient.connect();
        logger.info('Connected to database.');
        db = mongoClient.db(mongodbName);
        const exchange = db.collection('exchange');
        exchange.findOne({ name: 'oanda' }, (err, exchangeItem) => {
            if (err) {
                logger.error(err);
                return;
            }
            if (exchangeItem === null) {
                // Create exchange
                exchange.insertOne({
                    name: 'oanda',
                    heartbeat: new Date().getTime()
                }, (insertErr, exchangeInsert) => {
                    if (insertErr) {
                        logger.error(insertErr);
                        return;
                    }
                    exchangeId = exchangeInsert?.insertedId;
                    UpdateInstruments();
                });
            }
            else {
                dbConnected = true;
                exchangeId = exchangeItem?._id;
                UpdateInstruments();
            }
            logger.info('Connecting to oanda: ' + domain);
            request.end();
        })
    }
    catch (err) {
        logger.error(err);
    }
}

const UpdateInstruments = async () => {
  // Make sure all instruments are accounted for
    Object.keys(Timeframes).forEach(key => {
        Timeframes[key].exchangeId = exchangeId;
        const object = Timeframes[key];
        db.collection('timeframe').updateOne({
            exchange: exchangeName,
            symbol: Timeframes[key].symbol,
            timeframe: object.timeframe,
            exchangeId
        }, {
            $setOnInsert: object
        }, {
            upsert: true
        });
    });
    Object.keys(Instruments).forEach(key => {
        Instruments[key].exchangeId = exchangeId;
        const object = Instruments[key];
        db.collection('instrument').updateOne({
            exchange: exchangeName,
            symbol: object.symbol,
            marketSymbol: object.marketSymbol,
            exchangeId
        }, {
            $setOnInsert: object
        }, {
            upsert: true
        });
    })
}

// tslint:disable-next-line:no-shadowed-variable
const ProcessHeartbeat = (heartbeat: Date) => {
    logger.info(heartbeat);
    if (dbConnected === true) {
        const exhange = db.collection('exchange');
        exhange.updateOne({
            name: 'oanda'
        }, {
            $set: {
                heartbeat: new Date(heartbeat).getTime()
            }
        })
    }
};

const ProcessTicker = (ticker: Ticker) => {
    logger.info(JSON.stringify(ticker));
    const tick = db.collection('tick');
    tick.insertOne({
       symbol: ticker.symbol,
       bid: ticker.bid,
       ask: ticker.ask,
       epoch: new Date(ticker.timestamp).getTime(),
       timestamp: new Date(ticker.timestamp),
       exchangeId
    });
    db.collection('instrument').updateOne({
        symbol: ticker.symbol,
        exchangeId
    }, {
        $set: {
            bid: ticker.bid,
            ask: ticker.ask,
            epoch: new Date(ticker.timestamp).getTime(),
            timestamp: new Date(ticker.timestamp)
        }
    });
};

logger.info('Connecting to database.');
connect();