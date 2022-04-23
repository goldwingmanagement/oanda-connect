/*
Environment           <Domain>
fxTrade               stream-fxtrade.oanda.com
fxTrade Practice      stream-fxpractice.oanda.com
sandbox               stream-sandbox.oanda.com
*/
import { createLogger, format, transports } from 'winston';
import { Db, MongoClient } from 'mongodb';
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

const domain = 'stream-fxtrade.oanda.com';
const accessToken = process.env.APIKEY ? process.env.APIKEY : '';
const accountId = process.env.ACCOUNTID ? process.env.ACCOUNTID : '';
const mongodbUrl = process.env.MONGODBURL ? process.env.MONGODBURL : '';
const mongodbName = process.env.MONGODBNAME ? process.env.MONGODBNAME : '';

const mongoClient = new MongoClient(mongodbUrl);
let db: Db;
let dbConnected = false;

const instrumentList: string[] = process.env.INSTRUMENTS ? process.env.INSTRUMENTS.split(',') : [];
logger.info('Instruments: ' + instrumentList);
let instruments = '';
instrumentList.map(instrument => {
    instruments += instrument + '%2C';
});
instruments = instruments.substr(0, instruments.length - 3);

// tslint:disable-next-line:no-var-requires
const https = domain.indexOf('stream-sandbox') > -1 ? require('http') : require('https');
const options = {
    host: domain,
    path: '/v3/accounts/' + accountId + '/pricing/stream?instruments=' + instruments,
    method: 'GET',
    headers: {'Authorization' : 'Bearer ' + accessToken},
};

let heartbeat: Date;

interface Ticker {
    symbol: string,
    time: Date,
    bid: number,
    ask: number,
}
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
                time: json.time,
                bid: json.closeoutBid,
                ask: json.closeoutAsk
            };
            ProcessTicker(ticker);
            break;
        }
    }
    catch (err) {
        logger.error(err);
    }
}

logger.info('Connecting to database.');
connect();

async function connect() {
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
                });
            }
            else {
                dbConnected = true;
            }
            logger.info('Connecting to oanda: ' + domain);
            request.end();
        })
    }
    catch (err) {
        logger.error(err);
    }
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
       timestamp: ticker.time
    });
};
