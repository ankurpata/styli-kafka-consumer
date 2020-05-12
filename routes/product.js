const express = require("express");
const {GraphQLClient} = require("graphql-request");
const router = express.Router();
const gqlUrl = "http://localhost:3000/graphql/";

const client = new GraphQLClient(gqlUrl, {
    headers: {
        "Authorization": "v9UQm84xHz36CEhUq-nkRyjwj6aGMvmSeSydHupthyM.rJPubmijFLIZ_ATVlWkAfGqU_2t3LdvZR-GIhfOXZfM"
    }
});

const addProduct = async (inp) => {
    let data;
    try {
        const mutation = `
        mutation createProduct($input: CreateProductInput!) {
            createProduct(input: $input) {
              product {
                _id
              }
            }
         }`
        data = await client.request(mutation, inp);
        return data;
    } catch (e) {
        console.log(e.message, ' ~~~Error saving~~~ ');
    }
}

const saveVariantForProduct = async (inp) => {
    let data;
    try {
        const mutation = `
        mutation createProductVariant($input: CreateProductVariantInput!) {
            createProductVariant(input: $input) {
              variant {
                _id
              }
            }
          }
        `;
        data = await client.request(mutation, inp);
        return data;
    } catch (e) {
        console.log(e.message, ' ~~~Error saving~~~ ');
    }
}

const getProduct = async (inp) => {
    let data;
    try {

        const query = ` 
              query products($shopIds: [ID]!, $productIds: [ID], $query: String, $first: ConnectionLimitInt, $offset: Int) {
            products(shopIds: $shopIds, productIds: $productIds, query: $query, first: $first, offset: $offset) {
              nodes {
                  _id
                title
                currentProductHash
                isVisible
                media {
                  URLs {
                    thumbnail
                  }
                }
                price {
                  range
                }
                publishedProductHash
                variants {
                  _id
                }
              }
              pageInfo {
                hasNextPage
              }
              totalCount
          }
        }`;
        data = await client.request(query, inp);
        return data;
    } catch (e) {
        console.log(e.message, '@getProductFn');
        return false;
    }

};

exports.getProduct = getProduct;
exports.addProduct = addProduct;
exports.saveVariantForProduct = saveVariantForProduct;
