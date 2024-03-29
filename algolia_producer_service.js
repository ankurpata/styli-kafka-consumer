const Kafka = require('node-rdkafka');


const AlgoliaProducer = async (rowsStr, topic, keyReq) => {
    try {

        const producer = new Kafka.Producer({
            'metadata.broker.list': 'localhost:9092',
	    'message.max.bytes': '15728640',
	    'dr_cb': true
        });

        //logging debug messages, if debug is enabled
        producer.on('event.log', function (log) {
            console.log(log);
        });

        //logging all errors
        producer.on('event.error', function (err) {
            console.error('Error from producer');
            console.error(err);
        });

        producer.on('delivery-report', function (err, report) {
            console.log('delivery-report: ' + JSON.stringify(report));
            counter++;
        });

        //Wait for the ready event before producing
        producer.on('ready', function (arg) {
            // console.log('producer ready.' + JSON.stringify(arg));
            const TOPIC = topic || "ALGOLIA_PRICE_UPDATE";
            const partition = -1;
            const key = keyReq || 'KEY-PRICE-AlGOLIA';
            let value = Buffer.from(rowsStr);
           console.log('Dispatching topic', TOPIC, rowsStr.length);
	   producer.produce(TOPIC, partition, value, key);
            return true;
        });

        producer.on('disconnected', function (arg) {
            console.log('producer disconnected. ' + JSON.stringify(arg));
        });

        //starting the producer
       await producer.connect();


    } catch (e) {
        console.log(e.message, '@exception@AlgoliaProducer');
        return false;
    }
}


/**
 *
 * @param ms
 * @returns {Promise<unknown>}
 */
async function wait(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}


exports.AlgoliaProducer = AlgoliaProducer;

