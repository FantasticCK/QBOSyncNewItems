Array.prototype.syncForEach = async function (callback) {
    for (let index = 0; index < this.length; index++) {
        await callback(this[index], index, this);
    }
};// do the job one by one

const {Client} = require("pg");
const moment = require("moment-timezone");
const Redis = require("ioredis");
const QuickBooks = require("node-quickbooks");
const https = require('https');

const default_timezone = "America/Los_Angeles";
const QBO_consumerKey = "Q0medGU96jopiYEvZO3kPWD7TDsC0M1AIiTT984vcttMSUDxiP";
const QBO_consumerSecret = "94RTGwwbluu8QRS9I2zcK4wKNDGZwOk0E2oayiCM";
const QBO_token = "eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..EhKRLT18Kx4rM-3uc0B9NA.VlEwcKpV4gGeqYXfnNPvhPKbbVA8zcpygSHzYIsr-eCf9qUeS7V7nozdwTTDJiVMUOIHK7TAsK8DNlf5iCmD54YyHLkJDcxN1DLWu34x1Dwipu578efvkXdBNh-ayLn7G2oUKuKKgWqFaqmMtFmt9Iw3oNPGlsyWzPN3gYNJmd9eJ6GbR4xgNoKgCzEX3Z5_HwqYr6fNkuvP3eqL0wZ1kjphV7X3m0Jg1pG8ZlyDIsXUxzLa6pQUCPw1A2s81rcBxYZIL1b9eMMRCwd8Ydsf-c63D202sMdDOwya2F7PPq8mphrAlXpds88N5kcT_ZSjHL0gYRuyv91alF0QIEAudbnwxm-AMV2BDHtj7-9O2iGhBbaPLHX1z4aPyx2IXskqKwpqgvm6FhasPr4ISvUJUAeesfJ02pNeJNkg67DezRBl7KxD7ab1d_Yb2QWmk974ylRVsLCfD-KmefuYfHhlrOLaezLg5WsC00GDfEEQiqKYj-p1DVENA28ZxQooX5HSziumzroOx_G2voxyZKsnmhyE25T48FZQW6N0qJQ1tN0JmbJImD-6DGzrO70oZkmLJTlI_C1VuEWkayj4BYcPtw2CaLdaP1-IjNCRX6gQ3ztwHKrOKm7jkq76VLMdKIKK3l-HmuMmEE-fnHzJoI6Lk536vH7GdCvf_lZ8qdrnoXezrzTyfcPtQGG6L2_kXS2W.eir-nHq-GZffcbnLL6SAgA";
const QBO_realmId = "123146321859629";
const QBO_refreshToken = "Q0115652881630JV15tBvjKdJAFKzw4aOdyHWWxvxU0EaNJYge";

const env = "local";
const date = moment().tz(default_timezone).add(-1, "months").toISOString();
const consumerKey = QBO_consumerKey;
const consumerSecret = QBO_consumerSecret;
const token = QBO_token;
const tokenSecret = false;
const realmId = QBO_realmId;
const useSandbox = false;
const debug = false;
const endpoint = "https://quickbooks.api.intuit.com/v3/company/";
const minorversion = 4;
const oauthversion = "2.0";

const pgClient = new Client({
    host: "68.190.202.83",
    port: 5432,
    user: "datawarehouseapi",
    password: "evebyeve2020",
    database: "datawarehouse_entity_layer"
});

const redis = new Redis(17104, "pub-redis-17104.us-central1-1-1.gce.garantiadata.com", {
    password: "Yo9WYbdo",
    dropBufferSupport: true,
    enableReadyCheck: true,
    enableOfflineQueue: true
});

const query = {
    text: `SELECT * FROM public.product ` +
        `WHERE created_at > '${date}' ` +
        `ORDER BY id DESC;`
};

const categoryRef = {
    "category/accessories": {
        "localParentRef": "29011",
        "localAssetAccRef": "73",
        "prodParentRef": "8",
        "prodAssetAccRef": "116"
    },
    "category/beauty/makeup": {
        "localParentRef": "29013",
        "localAssetAccRef": "74",
        "prodParentRef": "10",
        "prodAssetAccRef": "169"
    },
    "category/beauty/nails": {
        "localParentRef": "29014",
        "localAssetAccRef": "74",
        "prodParentRef": "11",
        "prodAssetAccRef": "169"
    },
    "category/beauty/skincare": {
        "localParentRef": "29015",
        "localAssetAccRef": "74",
        "prodParentRef": "12",
        "prodAssetAccRef": "169"
    },
    "category/beauty/tools-brushes": {
        "localParentRef": "29016",
        "localAssetAccRef": "74",
        "prodParentRef": "13",
        "prodAssetAccRef": "169"
    },
    "category/beauty/value-sets": {
        "localParentRef": "29017",
        "localAssetAccRef": "74",
        "prodParentRef": "14",
        "prodAssetAccRef": "169"
    },
    "category/clothing": {
        "localParentRef": "29018",
        "localAssetAccRef": "75",
        "prodParentRef": "15",
        "prodAssetAccRef": "170"
    },
    "category/home": {
        "localParentRef": "29019",
        "localAssetAccRef": "77",
        "prodParentRef": "16",
        "prodAssetAccRef": "124"
    },
    "category/kids": {
        "localParentRef": "29020",
        "localAssetAccRef": "77",
        "prodParentRef": "17",
        "prodAssetAccRef": "124"
    },
    "category/lingerie": {
        "localParentRef": "29021",
        "localAssetAccRef": "76",
        "prodParentRef": "18",
        "prodAssetAccRef": "121"
    },
    "category/lingerie/bras": {
        "localParentRef": "29022",
        "localAssetAccRef": "76",
        "prodParentRef": "19",
        "prodAssetAccRef": "121"
    },
    "category/lingerie/loungewear": {
        "localParentRef": "29023",
        "localAssetAccRef": "76",
        "prodParentRef": "20",
        "prodAssetAccRef": "121"
    },
    "category/lingerie/panties": {
        "localParentRef": "29024",
        "localAssetAccRef": "76",
        "prodParentRef": "21",
        "prodAssetAccRef": "121"
    },
    "category/lingerie/shapewear": {
        "localParentRef": "29025",
        "localAssetAccRef": "76",
        "prodParentRef": "22",
        "prodAssetAccRef": "121"
    },
    "category/lingerie/sleepwear": {
        "localParentRef": "29026",
        "localAssetAccRef": "76",
        "prodParentRef": "23",
        "prodAssetAccRef": "121"
    },
    "category/men": {
        "localParentRef": "29035",
        "localAssetAccRef": "77",
        "prodParentRef": "25",
        "prodAssetAccRef": "124"
    },
    "category/men/shoes": {
        "localParentRef": "29036",
        "localAssetAccRef": "77",
        "prodParentRef": "26",
        "prodAssetAccRef": "124"
    },
    "category/shoes": {
        "localParentRef": "29037",
        "localAssetAccRef": "77",
        "prodParentRef": "27",
        "prodAssetAccRef": "124"
    },
    "category/sports": {
        "localParentRef": "29038",
        "localAssetAccRef": "77",
        "prodParentRef": "28",
        "prodAssetAccRef": "124"
    }
};

const accountRef = {
    "localIncomeAccountRef": "34",
    "localExpenseAccountRef": "26",
    "prodIncomeAccountRef": "28",
    "prodExpenseAccountRef": "29",
};

async function syncNewItemsToQBO(req, res) {

    /**
     * get new QBO access token. Try twice if failed
     *
     * @param qbo
     * @returns {Promise}
     */
    function refreshQBOAccessToken(qbo) {
        return new Promise((resolve, reject) => {
            qbo.refreshAccessToken((err, res) => {
                if (err || res.error) {
                    qbo.refreshAccessToken((err, res) => {
                        if (err) {
                            reject(err);
                        } else if (res.error) {
                            reject(res.error);
                        } else {
                            console.log(`Successfully refreshed QBO access token.`);
                            resolve(res);
                        }
                    });
                } else {
                    console.log(`Successfully refreshed QBO access token.`);
                    resolve(res);
                }
            });
        });
    }

    /**
     * create new item in qbo
     *
     * @param qbo
     * @param item
     * @returns {Promise}
     */
    function createQBONewItem(qbo, item) {
        return new Promise((resolve, reject) => {
            qbo.createItem(item, (err, result) => {
                if (err && err.Fault.Error[0].code === '6240') {
                    console.log(`Item: ${item.Sku} already exists`);
                    resolve(null);
                } else if (err) {
                    console.log(JSON.stringify(err));
                    resolve(null);
                } else {
                    console.log(`Created item: ${item.Sku}`);
                    resolve(result);
                }
            });
        });
    }

    /**
     * fetch new items from DW using SQL query
     *
     * @param pgClient object
     * @param query
     * @returns {Promise}
     */
    function fetchItemsFromDW(pgClient, query) {
        return new Promise((resolve, reject) => {
            pgClient.query(query, (err, res) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(res.rows);
                }
            });
        })
    }

    /**
     * Transform DW item entity into QBO item entity
     *
     * @param newDWItems
     * @param env
     * @returns {Array}
     */
    function formatQBONewItems(newDWItems, env) {
        const newQBOItems = [];
        newDWItems.forEach(item => {
            if (item.category_path !== 'category/materials') {
                newQBOItems.push({
                    "Name": item.upc,
                    "Sku": item.sku,
                    "Description": item.name,
                    "SubItem": true,
                    "ParentRef": {
                        "value": categoryRef[item.category_path][`${env}ParentRef`]
                    },
                    "Taxable": true,
                    "Type": "Inventory",
                    "IncomeAccountRef": {
                        "value": accountRef[`${env}IncomeAccountRef`]
                        //"name": "Sales:Sales of Product Income"
                    },
                    "ExpenseAccountRef": {
                        "value": accountRef[`${env}ExpenseAccountRef`]
                        //"name": "Cost of Goods Sold"
                    },
                    "AssetAccountRef": {
                        "value": categoryRef[item.category_path][`${env}AssetAccRef`]
                    },
                    "TrackQtyOnHand": true,
                    "QtyOnHand": 0,
                    "InvStartDate": "2017-01-01",
                });
            }
        });
        return newQBOItems;
    }

    /**
     * Update QBO items cached in redis
     *
     */
    function updateItemsInRedis() {
        const url = env === "local" ?
            `https://shipit-test.appspot.com/api/Zhihua/qbo/update_items` :
            `https://shipit-production.appspot.com/api/Zhihua/qbo/update_items`;
        console.log(`Update items in Redis started.`);
        https.get(url, (res) => {
            console.log(`StatusCode: ${res.statusCode}, statusMessage: ${res.statusMessage}`);
        }).on("error", (err) => {
            console.log(JSON.stringify(err));
        });
    }

    await pgClient.connect();
    let newDWItems = await fetchItemsFromDW(pgClient, query);
    let newQBOItems = await formatQBONewItems(newDWItems, env);

    try {
        let refreshToken;
        refreshToken = JSON.parse(await redis.get(`quickbooks:${env}:refreshToken`));
        let qbo = new QuickBooks(
            consumerKey,
            consumerSecret,
            token,
            tokenSecret,
            realmId,
            useSandbox,
            debug,
            minorversion,
            oauthversion,
            refreshToken
        );

        await refreshQBOAccessToken(qbo);
        await redis.set(`quickbooks:${env}:refreshToken`, JSON.stringify(qbo.refreshToken));
        console.log(`QBO refresh token updated in Redis.`);

        await newQBOItems.syncForEach(async item => {
            await createQBONewItem(qbo, item);
        });
        updateItemsInRedis();
        res.status(200).send(`QBO sync new items started!`);
    } catch (err) {
        console.log(JSON.stringify(err));
        res.status(500).send(`QBO sync new items error: ${err}`);
    }
}

exports.syncNewItemsToQBO = syncNewItemsToQBO;
