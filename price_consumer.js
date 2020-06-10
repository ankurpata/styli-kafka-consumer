const Kafka = require('node-rdkafka');
const util = require('util');
const express = require('express');
const path = require("path");
const cookieParser = require("cookie-parser");
const cors = require("cors");
require('dotenv').config();
const _ = require('lodash');
const csv = require('csvtojson')
const {GraphQLClient} = require("graphql-request");
const gqlUrl = "http://localhost:3000/graphql/";
const attrCache = {};
const {AlgoliaProducer} = require("./algolia_producer_service.js");
const client = new GraphQLClient(gqlUrl, {
    headers: {
        "Authorization": process.env.AUTH_KEY
    }
});

const shopId = process.env.SHOPID;

const bodyParser = require('body-parser');
const app = express();
const product = require("./routes/product");

const kafkaConf = {
    "group.id": "librd-test",
    "metadata.broker.list": "localhost:9092",
    "socket.keepalive.enable": true,
    'enable.auto.commit': true,
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
    const topics = [`PRICE_REVISION`];
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
        // if (counter % numMessages === 0) {
        //     console.log("calling commit");
        consumer.commit(m);
        // }
        let msgStr = m.value.toString();
        const payload = JSON.parse(msgStr);
        let intNo = payload.intNo;
        console.log(msgStr, '~~~~~~~Kafka Stream Response~~~~~~~');

        /**
         * Save New Prices for SKUs
         */
        let batchUpdateArr = payload.data.map(([sku, price, special_price]) => ({
            sku,
            price,
            special_price,
        }));
       
console.log(batchUpdateArr, 'batchUpdateArr', shopId,' shopId ');

        const inp = {
            input: {
                shopId,
                data: batchUpdateArr
            }
        };
        const uploadRes = await product.updateProductBySku(inp);
        console.log("~~~~~~~Saved in Reaction Catalog~~~~~~~~~", uploadRes);

        //Dispatch action on Kafka Topic to update Algolia
        AlgoliaProducer(msgStr);


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

app.listen(4568, () => {
    console.log("Price consumer Server is listening to port 4568");
})


const getAttributeGroups = async (attributeSetCode) => {
    const {getAttributeGroups: {attributeGroups}} = await client.request(AttributeGroupMappingGQL, {
        attributeSetId: attributeSetCode,
        shopId
    });
    console.log(attributeGroups, 'attributeGroups');
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
