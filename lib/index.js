"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/*
Environment           <Domain>
fxTrade               stream-fxtrade.oanda.com
fxTrade Practice      stream-fxpractice.oanda.com
sandbox               stream-sandbox.oanda.com
*/
var winston_1 = require("winston");
var combine = winston_1.format.combine, timestamp = winston_1.format.timestamp, printf = winston_1.format.printf;
// tslint:disable-next-line:no-shadowed-variable
var myFormat = printf(function (_a) {
    var level = _a.level, message = _a.message, timestamp = _a.timestamp;
    return "".concat(timestamp, " ").concat(level, ": ").concat(message);
});
var logger = (0, winston_1.createLogger)({
    format: combine(timestamp(), myFormat),
    transports: [new winston_1.transports.Console()]
});
// eslint-disable-next-line @typescript-eslint/no-var-requires
require('dotenv').config();
var domain = 'stream-fxtrade.oanda.com';
var accessToken = process.env.APIKEY;
var accountId = process.env.ACCOUNTID;
var instrumentList = process.env.INSTRUMENTS ? process.env.INSTRUMENTS.split(',') : [];
logger.info('Instruments: ' + instrumentList);
var instruments = '';
instrumentList.map(function (instrument) {
    instruments += instrument + '%2C';
});
instruments = instruments.substr(0, instruments.length - 3);
// tslint:disable-next-line:no-var-requires
var https = domain.indexOf('stream-sandbox') > -1 ? require('http') : require('https');
var options = {
    host: domain,
    path: '/v3/accounts/' + accountId + '/pricing/stream?instruments=' + instruments,
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + accessToken },
};
logger.info('Connecting to: ' + domain);
var heartbeat;
var request = https.request(options, function (response) {
    var bodyChunk = '';
    response.on('data', function (chunk) {
        bodyChunk += chunk.toString().trim();
        if (bodyChunk.endsWith('}')) {
            var data = bodyChunk;
            bodyChunk = '';
            var parts = data.split('\n');
            parts.forEach(function (part) {
                if (part === '' || part === null)
                    return;
                try {
                    var json = JSON.parse(part);
                    switch (json.type) {
                        case 'HEARTBEAT':
                            heartbeat = new Date(json.time);
                            ProcessHeartbeat(heartbeat);
                            break;
                        case 'PRICE':
                            // eslint-disable-next-line no-case-declarations
                            var ticker = {
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
            });
        }
    });
    response.on('end', function () {
        logger.error('Error connecting to OANDA HTTP Rates Server');
        logger.error(' - HTTP - ' + response.statusCode);
        // Connection is bad, stop process, if ran in Docker, this should restart the process
        process.exit(1);
    });
});
// tslint:disable-next-line:no-shadowed-variable
var ProcessHeartbeat = function (heartbeat) {
    logger.info(heartbeat);
};
var ProcessTicker = function (ticker) {
    logger.info(JSON.stringify(ticker));
};
request.end();
