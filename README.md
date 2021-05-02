# dynadash

### Helpers

+ `getTableRow` - get single row
```javascript
getTableRow(
    tableName, // string
    keys, // { hash: "key", range: "key" },
    projection?, // string[], (optional)
    consistent? // boolean, (optional)
)
```
+ `queryTableIndex` - query table index
```javascript
queryTableIndex(
    tableName, // string
    indexName, // string

    // Simplified mappings to generate `KeyConditionExpression` and `FilterExpression`.
    // key/val pair generates a simple equality condition, use an object to specify operator and value if needed.
    keyCondExpressionMap, // { hash: 'test', range: { op: '>=', value: 1234 }}
    filterExpressionMap // same as ^, with addition of "<>" operator
)
```

+ `batchPutTable`, `batchDelTable` - generate bulk write requests, chunks of 25 each, auto retry (10x max) w/ exponential backoff. (2s - 4min)
```javascript
batchPutTable(
    tableName, // string
    unmarshalledList, // any[],
    predicate?, // (item) => any  // called for every element of `unmarshalledList` (optional)
)
```

* `putTableRow`, `delTableRow` - update or delete single item
```javascript
putTableRow(tableName, item)
delTableRow(tableName, key)
```


+ `shallowUpdateTableRow` - shallow update 1st level elements
```javascript
shallowUpdateTableRow(
    tableName, // string
    keys, // { hash: "key", range: "key" }
    row, // { attr: "newValue", attr2: "newVal" }
)
```
