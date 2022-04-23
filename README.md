# Oanda V20 Socket

## Notice
This code is reformatted from Oanda's websocket code to work with their latest v20 service.  We are not responsible with maintaining this code.

## Installation

```
npm install
```

## Requirements
You will need an API key which you can get from https://www.oanda.com/account/tpa/personal_token.  Additionally, your accound id will be you v20 id from https://www.oanda.com/us-en/trading/instruments/.

## Configuration
Create an .env and update the following:

```
ACCOUNTID=
APIKEY=
INSTRUMENTS=
```
https://www.oanda.com/us-en/trading/instruments/
*Instruments are in QUOTE_BASE format, separate with comma

## Running

```
npm start
```