const Kafka = require('node-rdkafka');
const util = require('util');
const express = require('express');
const path = require("path");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const _ = require('lodash');
const CSV = require('csv-string');

const bodyParser = require('body-parser');
const app = express();
const product = require("./routes/product");

const kafkaConf = {
    "group.id": "librd-test",
    "metadata.broker.list": "localhost:9092",
    "socket.keepalive.enable": true,
    'enable.auto.commit': false,
    "debug": "generic,broker,security"
};

// const kafkaConf = {
//     "group.id": "cloudkarafka-example",
//     "metadata.broker.list": "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094".split(","),
//     "socket.keepalive.enable": true,
//     "security.protocol": "SASL_SSL",
//     "sasl.mechanisms": "SCRAM-SHA-256",
//     "sasl.username": "bddcy39c",
//     "sasl.password": "e8hPouz3LL2rhp_vtQhp547rYsr9BbhQ",
//     "debug": "generic,broker,security"
// };

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));
app.use(cookieParser());
app.use(cors());
// app.use("/product", productRouter);
// catch 404 and forward to error handler
app.use(function (req, res, next) {
    next(createError(404));
});


try {
    console.log("kafka consumer is booting up")

    const topics = [`CSV_PRODUCTS`];
    const consumer = new Kafka.KafkaConsumer(kafkaConf, {
        "auto.offset.reset": "beginning"
    });


    const numMessages = 5;
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
        if (counter % numMessages === 0) {
            console.log("calling commit");
            consumer.commit(m);
        }
        let msgStr = m.value.toString();
        mstStr= JSON.parse(msgStr);
        console.log(msgStr, 'msgStr');
        throw Error('Intentional Break');
        msgStr = CSV.parse(msgStr);
        // console.log(msgStr, 'msgStr');
        let newProducts = csv2json(msgStr, ',');
        console.log(msgStr, 'msgStr', newProducts);

        /**
         * Save New Products
         */
        for (const productObj of newProducts) {
            let variantObj = {};
            let variants = productObj.variants;
            const configurableVariations = productObj.configurableVariations;
            productObj.isVisible = true;
            productObj.title = productObj.name;
            productObj.originalPrice = productObj.originalBasePrice;
            variantObj.weight = productObj.weight;
            variantObj.price = productObj.price;
            const productOnline = productObj.productOnline;
            delete productObj.sku;
            delete productObj.categories;
            delete productObj.name;
            delete productObj.weight;
            delete productObj.price;
            delete productObj.createdAt;
            delete productObj.updatedAt;
            delete productObj.productOnline;
            delete productObj.originalBasePrice;
            delete productObj.variants;

            //Save product//
            const input = {
                input: {
                    shopId: "cmVhY3Rpb24vc2hvcDpzOU1jWGVvQndEYTIzQW1Ldw",
                    product: productObj,
                    shouldCreateFirstVariant: false
                }
            }
            const {createProduct: {product: {_id}}} = await product.addProduct(input);
            console.log(`Saved product, _id: ${_id}`);
            console.log(JSON.stringify(_.map(variants, 'price')), 'variants');
            console.log(JSON.stringify(_.map(variants, 'sku')), 'variants');
            ////Save Variants////
            const configurableVariationsArr = configurableVariations.trim().split("|");
            if (configurableVariationsArr.length && variants.length) {

                let i = 0;
                const bulkVariants = [];
                for (const conf of configurableVariationsArr) {
                    let productVariant = {};
                    const confArr = conf.split(",");
                    for (const variation of confArr) {
                        const subVar = variation.split("=");
                        if (!subVar.length) {
                            continue;
                        }
                        const [key, value] = subVar;
                        if (key === "sku") {
                            // eslint-disable-next-line prefer-destructuring
                            productVariant.sku = subVar[1];
                        } else {
                            // TODO: Use metaifelds with attributes key for more custom attributes
                            productVariant.attributeLabel = key;
                            productVariant.optionTitle = value;
                        }
                    }
                    console.log('i, price', i, variants[i].price);
                    const relevantVariant = variants.find(x => x.sku === productVariant.sku);
                    productVariant.price = parseFloat(relevantVariant.price);
                    productVariant.isVisible = true;
                    productVariant.length = parseFloat(relevantVariant.length) || 0;
                    productVariant.weight = parseFloat(relevantVariant.weight) || 0;
                    // productVariant.sku = variants[i].sku;
                    productVariant.metafields = [];
                    // productVariant.metafields.push({"additionalAttributes": variants[i].additionalAttributes});
                    ////Save product variant////
                    // const input = {
                    //     input: {
                    //         shopId: "cmVhY3Rpb24vc2hvcDpzOU1jWGVvQndEYTIzQW1Ldw",
                    //         productId: _id,
                    //         variant: productVariant
                    //     }
                    // }
                    // const variant = await product.saveVariantForProduct(input);
                    // console.log(`Saved variant, SKU: ${productVariant.sku}`);
                    bulkVariants.push(productVariant);
                    i++;
                }

                //Save using bulk variants
                const input = {
                    input: {
                        shopId: "cmVhY3Rpb24vc2hvcDpzOU1jWGVvQndEYTIzQW1Ldw",
                        productId: _id,
                        variants: bulkVariants
                    }
                }
                const variantsRes = await product.saveBulkVariants(input);
                console.log(`Saved variant, SKU: ${ JSON.stringify(variantsRes)}`);

            } else {
                //Its a simple product//
                //Add a defualt variant based on product Params.
            }
        }
        /**
         * GraphQl queries
         */
        // const inp = {
        //     shopIds: ["cmVhY3Rpb24vc2hvcDpzOU1jWGVvQndEYTIzQW1Ldw"],
        //     first: 10,
        //     limit: 10,
        //     offset: 0
        // };
        // const productList = await product.getProduct(inp);
        // console.log(productList, 'productList');

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

app.listen(4569, () => {
    console.log("Server is listening to port 4569");
})


const csv2json = (strArr, delimiter = ', ') => {
    let titles = strArr[0];
    titles = titles.map((v, k) => _.camelCase(v));

    delete strArr[0];
    const rows = strArr;
    let tmpVariants = [];
    let currParentSku = "";
    let res = [];
    rows.map(row => {
        const values = row;
        const retArr = titles.reduce((object, curr, i) => (object[curr] = values[i], object), {});
        let currSku = retArr['sku'];
        if (currParentSku == currSku) {
            //product
            retArr.variants = tmpVariants;
            tmpVariants = [];
            currParentSku = "";
            res.push(retArr);
        } else {
            //variant
            currParentSku = ("" + currSku).slice(0, -2);
            console.log(currParentSku, 'currParentSku', currSku);
            tmpVariants.push(retArr);
        }
    });
    return res;
};

const groupProducts = (arr) => {

}
