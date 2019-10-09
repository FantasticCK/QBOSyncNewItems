const fs = require('fs');
const csv = require('csv-parser');

const categoryMapping = {};

fs.createReadStream('MAPPING.csv').pipe(csv()).on('data', (row) => {
    const value = {
        "localParentRef": row.localParentRef,
        "localAssetAccRef": row.localAssetAccRef,
        "prodParentRef": row.prodParentRef,
        "prodAssetAccRef": row.prodAssetAccRef,
    };
    const key = row.category_path;
    categoryMapping[key] = value;
}).on('end', () => {
    console.log(`MAPPING.csv has been imported!`);
});

