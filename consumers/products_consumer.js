const Kafka = require('node-rdkafka');
const util = require('util');
const express = require('express');
const path = require("path");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const _ = require('lodash');
require('dotenv').config()
const csv = require('csvtojson')
const {GraphQLClient} = require("graphql-request");
const gqlUrl = "http://localhost:3000/graphql/";
const attrCache = {};

const client = new GraphQLClient(gqlUrl, {
    headers: {
        "Authorization": process.env.AUTH_KEY
    }
});

const shopId = process.env.SHOPID;
const {AlgoliaProducer} = require("../producers/algolia_producer_service.js");

const bodyParser = require('body-parser');
const app = express();
const product = require("../api/product");

const kafkaConf = {
    "group.id": "librd-test",
    "fetch.message.max.bytes": "15728640",
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

app.use(bodyParser.json({limit: '10mb', extended: true}));
app.use(bodyParser.urlencoded({limit: '10mb', extended: true}));
// app.use(appo.getMiddleware({ path: '/graphql' });


// app.use(bodyParser.urlencoded({extended: true}));
// app.use(bodyParser.json({extended: true}));
app.use(cookieParser());
app.use(cors());
// app.use("/product", productRouter);
// catch 404 and forward to error handler
app.use(function (req, res, next) {
    next(createError(404));
});


try {
    console.log("kafka consumer is booting up")

    // const topics = [`bddcy39c-default`];
    const topics = [`IMPORT_CSV`];
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
        console.log(msgStr, 'msgStr', JSON.parse(msgStr));
        // throw Error('Intentional Break');
        let payload = JSON.parse(msgStr);
        let intNo = payload.intNo;
        let filePath = "./08.04.2020Working.csv";
        if (payload['value'] == 'UPLOAD_CSV_BIG') {
            filePath = "./bulkCsv.csv";
        }
        console.log(filePath, 'filePath')
        const newProducts = await csv().fromFile(filePath);

        //console.log(newProducts, 'newProducts');
        // let newProducts = csv2json(msgStr, ',');
        // console.log(msgStr, 'msgStr', newProducts);
        // throw Error('Intentional Break');

        // Process data rows.
        let tmpVariants = [];
        let currParentSku = "";
        const bulkCreateProductInput = [];

        for (let outputarray of newProducts) {

            let output2 = {};
            for (const key of Object.keys(outputarray)) {
                output2[_.camelCase(key)] = outputarray[key];
            }

            outputarray = output2;

            // Process Row Array
            // const productVariantSet = await mapAndInsertProduct(outputarray);
            const currSku = outputarray.sku;
            if (currParentSku == currSku) {
                // product
                outputarray.variants = tmpVariants;
                tmpVariants = [];
                currParentSku = "";
                outputarray = await formatProductObj(outputarray);
                bulkCreateProductInput.push(outputarray);
            } else {
                // variant
                currParentSku = (`${currSku}`).slice(0, -2);
                // eslint-disable-next-line no-console
                tmpVariants.push(outputarray);
            }
        }
        console.log(bulkCreateProductInput.length, 'newProducts');
        // throw Error('Intentional Break');


        //Break total into chunks of batch 200.
        const chunks = _.chunk(bulkCreateProductInput, 400);
        console.log(`Broken full data into chunk of ${chunks.length}`);
        for (const chunk of chunks) {
            /**
             * Save New Products
             */
            const inp = {
                input: {
                    shopId,
                    data: chunk
                }
            };
            console.log('creating bulk product');
            //           const productIds = await product.createBulkProductFn(inp);
            //        console.log('created',{productIds});

            //Dispatch to Kafka. Call Producer
            const algoliaData = chunk.map((productVariant) => {
                const {product, variants} = productVariant;
                let productSku = variants[0].sku.substring(0, variants[0].sku.length - 2);
                return {
                    sku: productSku,
                    title: product.title,
                    variantSize: variants.length,
                    attributeSize: product.metafields.length
                }
            });
            const prodPayload = {data: algoliaData, intNo};
            console.log(prodPayload, 'produc tpayload');
            AlgoliaProducer(JSON.stringify(prodPayload), "ALGOLIA_PRODUCT_UPDATE", "KEY-PRORDUCT-AlGOLIA");
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

app.listen(4569, () => {
    console.log("Server is listening to port 4569");
})


const getAttributeGroups = async (attributeSetCode) => {
    const {getAttributeGroups: {attributeGroups}} = await client.request(AttributeGroupMappingGQL, {
        attributeSetId: attributeSetCode,
        shopId
    });
    // console.log(attributeGroups, 'attributeGroups');
    return attributeGroups;
};

const getMetaFields = async (row) => {
    const {attributeSetCode, additionalAttributes} = row;
    const additionalAttrArr = additionalAttributes.split(",");
    additionalAttrArr.map((val) => {
        const subVar = val.split("=");
        const [l, r] = subVar;
        row[l] = r;
    });


    const attributeGroups = attrCache[attributeSetCode] || await getAttributeGroups(attributeSetCode);
    attrCache[attributeSetCode] = attributeGroups;
    if (!attributeGroups || !attributeGroups.length) {
        return [];
    }
    return attributeGroups.map((attrGroup) => {
        const metaKey = attrGroup.attributeGroupLabel.trim()
            .split(" ")
            .join("-");
        const {attributes} = attrGroup;
        const attributeValueObj = {};
        attributes.map((attribute) => {
            attributeValueObj[attribute.label] = row[attribute.label];
        });
        // TODO: Remove stringify and use object
        return {
            key: metaKey,
            value: JSON.stringify(attributeValueObj)
        };
    });
};

const formatProductObj = async (productObj) => {
    const metafields = await getMetaFields(productObj);
    const {variants, configurableVariations, attributeSetCode} = productObj;
    const product = {
        title: productObj.name,
        metafields,
        isVisible: true,
        attributeSetCode,
        productType: productObj.productType
    };
    const configurableVariationsArr = configurableVariations.trim()
        .split("|");
    const bulkVariants = [];

    if (configurableVariationsArr.length && variants.length) {
        for (const conf of configurableVariationsArr) {
            const productVariant = {};
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
                    productVariant.attributeLabel = key;
                    productVariant.optionTitle = value;
                }
            }
            const relevantVariant = variants.find((x) => x.sku === productVariant.sku);
            productVariant.price = parseFloat(relevantVariant.price);
            productVariant.isVisible = true;
            productVariant.length = parseFloat(relevantVariant.length) || 0;
            productVariant.weight = parseFloat(relevantVariant.weight) || 0;
            productVariant.metafields = await getMetaFields(productObj);
            bulkVariants.push(productVariant);
        }
    }
    return {
        product,
        variants: bulkVariants
    };
};


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

const AttributeGroupMappingGQL = `
    query getAttributeGroups($attributeSetId: ID!, $shopId: ID!) {
        getAttributeGroups(input:{ attributeSetId : $attributeSetId , shopId:$shopId} ) {
            attributeGroups{attributes{label, id} , attributeGroupId , attributeGroupLabel}
        }
    }
`;
