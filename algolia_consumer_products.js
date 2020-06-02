const Kafka = require('node-rdkafka');
const express = require('express');
const cookieParser = require("cookie-parser");
const cors = require("cors");
const _ = require('lodash');
const axios = require('axios');


const bodyParser = require('body-parser');
const app = express();
const algoliasearch = require('algoliasearch');
// const client = algoliasearch('testing20LAJL5O48', '35175c94ce10c6b8f8ccbd57f814e62c');
const client = algoliasearch('testing20LAJL5O48', '8372234dcaf9e8dd3515dc962c9e6c37');

const kafkaConf = {
    "group.id": "librd-test",
    "metadata.broker.list": "localhost:9092",
    "socket.keepalive.enable": true,
    'enable.auto.commit': false,
    "debug": "generic,broker,security"
};

app.use(bodyParser.json({limit: '10mb', extended: true}));
app.use(bodyParser.urlencoded({limit: '10mb', extended: true}));
app.use(cookieParser());
app.use(cors());
app.use(function (req, res, next) {
    next(createError(404));
});


try {
    console.log("kafka Algolia consumer is booting up")

    // const topics = [`bddcy39c-default`];
    const topics = [`ALGOLIA_PRODUCT_UPDATE`];
    const consumer = new Kafka.KafkaConsumer(kafkaConf, {
        "auto.offset.reset": "beginning"
    });

    const numMessages = 1;
    let counter = 0;
    consumer.on("error", function (err) {
        console.error(err);
    });
    consumer.on("ready", function (arg) {
        console.log(`Consumer ${arg.name} ready`);
        consumer.subscribe(topics);
        consumer.consume();
    });
    consumer.on("data", async function (m) {
        counter++;
        // if (counter % numMessages === 0) {
        //     console.log("calling commit");
        consumer.commit(m);
        // }
        let msgStr = m.value.toString();
        console.log(msgStr.length, '~~~~~~~Kafka Stream Product Response~~~~~~~');
        const payload = JSON.parse(msgStr);
        let intNo  = payload.intNo;
        const updateArray = payload.data;
        console.log(updateArray[1], 'updateArray[1]');
        //Push updates to Algolia

        const index = client.initIndex('reaction_kafka_product');
        updateArray.shift();
        const batchUpdateArr = updateArray.map((productVariant) => {
            const {product, variants} = productVariant;
            let productSku = variants[0].sku.substring(0, variants[0].sku.length - 2);
            return {
                sku: productSku,
                title:product.title,
                variantSize: variants.length,
                attributeSize: product.metafields.length
            }
        });
        console.log(batchUpdateArr.length, ': Size of batchUpdateArr', batchUpdateArr.length);

        // throw Error("Exception Intentionally");

        // const mapper = {};
        // for (const priceArr of updateArrayTest) {
        //     let objectId = "";
        //     if (mapper[priceArr[0]]) {
        //         objectId = mapper[priceArr[0]];
        //         continue;
        //     } else {
        //         const {hits} = await index.search(priceArr[0], {attributesToRetrieve: ['objectID']});
        //         objectId = hits[0].objectID;
        //         mapper[objectId] = 1;
        //     }
        //     console.log('objectId', objectId);
        //     batchUpdateArr.push({
        //         action: 'partialUpdateObject',
        //         indexName: 'magento2_dev_en_products',
        //         body: {
        //             objectID: objectId,
        //             price: {
        //                 "SAR": {
        //                     default: priceArr[1],
        //                     default_formated: `SAR ${priceArr[1]}.00`,
        //                     special_from_date: false,
        //                     special_to_date: false
        //                 }
        //             }
        //         }
        //     });
        // }
        try {
            const res = await index.saveObjects(batchUpdateArr, {autoGenerateObjectIDIfNotExist: true});
            console.log(' Res Bulk products UPSERT SAVE ');
        } catch (e) {
            console.log(e.message, 'e@message,Error');
        }
        console.log("~~~~~ Done batch ~~~~", updateArray.length);


        /**
         * Log time after saving to algolia
         */
        let i = 1;
        const logParams = {
            "iterationName": "csv_produts_save",
            "iterationNumber": intNo,
            "numRecords": batchUpdateArr.length,
            "execTime1": -1,
            "startTime": new Date().toISOString(),
            "endTime": new Date().toISOString(),
            "batchNumber": 1,
            "itemsPerBatch": batchUpdateArr.length,
            "execTime2": "-1",
            "platform": "reaction"
        };

        try {

            //Async post and do not wait for response.
            const {data: logRes} = await axios.post('https://us-central1-stylishopdev.cloudfunctions.net/perf-monitor', logParams);
            console.log(batchUpdateArr.length, logParams, 'Read batchUpdateArr CSV', ". Dispatching product updates data. logRes: ", logRes);
        } catch (e) {
            console.log(e.message, 'Error logging')
        }
    });
    consumer.on("disconnected", function (arg) {
        process.exit();
    });
    consumer.on('event.error', function (err) {
        console.error(err);
        process.exit(1);
    });
    consumer.on('event.log', function (log) {
        console.log(log);
    });
    consumer.connect();


} catch (e) {
    console.log(e);
    throw Error(e);
}

app.listen(4566, () => {
    console.log("Price consumer Server is listening to port 4568");
})



