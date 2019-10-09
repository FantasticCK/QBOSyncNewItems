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

//require('dotenv').config();
const default_timezone = "America/Los_Angeles";
const QBO_consumerKey = "Q0nwvpTTtGuROBVU3sg3KHEmleExwYbuTSH6cV28pzcbsq3hkK";
const QBO_consumerSecret = "EhmX8wXni8IfO9XRwwrWg5z8lvOikcpKMRIsC36F";
const QBO_token = "eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..dQ05fiVWXeO08FutLe5yug.or2Q9tZj9J81pVTn_NyroNwdaXRqveLGToC1hMoWZxHWwQqC4lziC2X0jEK_UGuiRSU_CANVwaKzXKW2qLPj5o9JBa7nefYvdHRtajCKlrOZXMGME6cc7nTxK7MbV9GLDiHtUP4rKcckrYXJb1sXC8JHvyq5Ya0YtJI4OLrxCBSQJgFEzojhehbhdUUzOTn_K8TJ61QijSw6xEEzc6A7Q1VETiKRIbAQ6NuTlxL1GW5PNzy1rAWgWRo05n7YHbnDkBxM6Ra9yzElE5Pf-hvWiuJdefHJ3nkaF87tvFpwhNFDT02Wkt-4SvDYFIioawX326u5Ut7RihPSLQmYR0EME72yqvJbQz9x33E7a2TGCToT3q-aI4enriVUH39x5OSFWLNDwS2Ky6gnap-L0MjE_s5xPt-glisYIokDiav-87qKQy8oqydsGjy5cs04hxJ3BVEy5o2ra-ae3ls-L-IEervPD5VhtmPi6jX4PaZMPpAbJuJLxkEpjTzP0RWf78eiBeyQbuxgF-UwcOo3tVYuJJehtAqiRI5uKfzuoxelEaRV8zUjNKlffPKEdfnfnsEBuVR7BrfBmO5KFAhjlTFTZmCUIChflAU7xi9XbSSzAc45KMk-tVkHW_h46GIJP19LbWlh8OyqYFzJv-vlODe_ZSdBzVyo0fl5gQDz6nrXg8QWXRtFeefND4b6kOovePzR.6JdFunn5dX1MaQJtu991IQ";
const QBO_realmId = "123146391441304";
const QBO_refreshToken = "Q011565283274wRMLJxnNoDN13Y3pZHQOxhwssEa0GHugBRAup";

const env = "prod";
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
                        resolve(res);
                    }
                });
            } else {
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
    const url = env === 'local' ?
        `https://shipit-test.appspot.com/api/Zhihua/qbo/update_items` :
        `https://shipit-production.appspot.com/api/Zhihua/qbo/update_items`;
    https.get(url, (res) => {
        console.log(`statusCode: ${res.statusCode}, statusMessage: ${res.statusMessage}`);
    }).on("error", (err) => {
        console.log(JSON.stringify(err));
    });
}

async function syncNewItemsToQBO() {
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
        await newQBOItems.syncForEach(async item => {
            await createQBONewItem(qbo, item);
        });
        updateItemsInRedis();

    } catch (err) {
        console.log(JSON.stringify(err));
    }
}

syncNewItemsToQBO();

// function findQBOItems(qbo, criteria) {
//     return new Promise((resolve, reject) => {
//         qbo.findItems(criteria, (err, res) => {
//             if (err) {
//                 reject(err);
//             } else if (!res.QueryResponse.Item) {
//                 reject(null);
//             } else {
//                 resolve(res.QueryResponse.Item);
//             }
//         });
//     });
// }
//
// function findQboAccounts(qbo, criteria) {
//     return new Promise((resolve, reject) => {
//         qbo.findAccounts(criteria, (err, res) => {
//             if (err) {
//                 reject(err);
//             } else if (!res.QueryResponse.Account) {
//                 reject(null);
//             } else {
//                 resolve(res.QueryResponse.Account);
//             }
//         });
//     });
// }
