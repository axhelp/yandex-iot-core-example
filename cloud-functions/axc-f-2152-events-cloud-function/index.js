const https = require('https');
const querystring = require('querystring');
const fs = require('fs');
const ClickhouseConnectOptions = {
    // hostname: 'rc1c-xxxx123456.mdb.yandexcloud.net',
    hostname: 'xx.xx.xx.xx',
    port: 8443,
};

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// const user = process.env.USER;
// const password = process.env.PASSWORD;

exports.handler = async function (event, context) {
    const access_token = context.token.access_token;

    event.messages.forEach(message => {
        const now = new Date();
        const dt = `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`;
        const ts = Math.floor(now / 1000);
        const messageText = Buffer.from(message.details.payload, 'base64').toString('utf-8');
        const cleanedMessageText = messageText.replace(/[^a-zA-Z0-9"{}:,/]/g, '');

        try {
            const messageObject = JSON.parse(cleanedMessageText);
            const {value, topic} = messageObject;

            const options = {
                'method': 'POST',
                'ca': fs.readFileSync(
                    'YandexInternalRootCA.crt'
                ),
                'path': '/?' + querystring.stringify({
                    'database': 'pxc_cloud_db',
                    'query': `INSERT INTO pxc_cloud_db.timeseries_example (dt, ts, topic, value) VALUES ('${dt}', ${ts}, '${topic}', ${value})`,
                }),
                'port': ClickhouseConnectOptions.port,
                'hostname': ClickhouseConnectOptions.hostname,
                'headers': {
                    // 'X-ClickHouse-User': user,
                    // 'X-ClickHouse-Key': password,
                    'Authorization': `Bearer ${access_token}`,
                },
            };

            const req = https.request(options, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk) => {
                    console.log(chunk);
                });
            });

            req.end();
        } catch (error) {
            console.log(messageText);
            console.error(error);
        }
    });

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'isBase64Encoded': false,
        'body': event
    }
};
