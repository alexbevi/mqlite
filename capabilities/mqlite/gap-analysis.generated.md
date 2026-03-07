# MongoDB Capability Gap Analysis

This report is generated from MongoDB source registries for query and aggregation capabilities.
The command IDL files are included as reference anchors for command shapes, but the operator and stage lists come from the source registries that actually register them.

Resync from a sibling `mongo` checkout with:

```text
cargo run -p mqlite-capabilities -- sync
```

Check the checked-in artifacts without rewriting them with:

```text
cargo run -p mqlite-capabilities -- sync --check
```

## Reference Anchors

- `../mongo/src/mongo/db/query/compiler/parsers/matcher/expression_parser.cpp`
- `../mongo/src/mongo/db/pipeline/expression.cpp`
- `../mongo/src/mongo/db/pipeline`
- `../mongo/src/mongo/db/query/find_command.idl`
- `../mongo/src/mongo/db/pipeline/aggregate_command.idl`
- `../mongo/src/mongo/db/query/write_ops/write_ops.idl`

## Summary

| Category | Public upstream | Supported | Unsupported |
| --- | ---: | ---: | ---: |
| Query operators | 38 | 29 | 9 |
| Aggregation stages | 57 | 21 | 36 |
| Aggregation expression operators | 176 | 11 | 165 |
| Aggregation accumulators | 25 | 5 | 20 |
| Aggregation window functions | 36 | 0 | 36 |

## Query Operators

Public upstream: 38. Supported: 29. Unsupported: 9.

### Supported Public

- `$all`
- `$alwaysFalse`
- `$alwaysTrue`
- `$and`
- `$bitsAllClear`
- `$bitsAllSet`
- `$bitsAnyClear`
- `$bitsAnySet`
- `$comment`
- `$elemMatch`
- `$eq`
- `$exists`
- `$expr`
- `$gt`
- `$gte`
- `$in`
- `$lt`
- `$lte`
- `$mod`
- `$ne`
- `$nin`
- `$nor`
- `$not`
- `$options`
- `$or`
- `$regex`
- `$sampleRate`
- `$size`
- `$type`

### Unsupported Public

- `$geoIntersects`
- `$geoNear`
- `$geoWithin`
- `$jsonSchema`
- `$near`
- `$nearSphere`
- `$text`
- `$where`
- `$within`

### Internal Or Server-Only Upstream

- `$_internalBucketGeoWithin`
- `$_internalEqHash`
- `$_internalExprEq`
- `$_internalExprGt`
- `$_internalExprGte`
- `$_internalExprLt`
- `$_internalExprLte`
- `$_internalPath`
- `$_internalSchemaAllElemMatchFromIndex`
- `$_internalSchemaAllowedProperties`
- `$_internalSchemaBinDataEncryptedType`
- `$_internalSchemaBinDataSubType`
- `$_internalSchemaCond`
- `$_internalSchemaEq`
- `$_internalSchemaFmod`
- `$_internalSchemaMatchArrayIndex`
- `$_internalSchemaMaxItems`
- `$_internalSchemaMaxLength`
- `$_internalSchemaMaxProperties`
- `$_internalSchemaMinItems`
- `$_internalSchemaMinLength`
- `$_internalSchemaMinProperties`
- `$_internalSchemaObjectMatch`
- `$_internalSchemaRootDocEq`
- `$_internalSchemaType`
- `$_internalSchemaUniqueItems`
- `$_internalSchemaXor`

## Aggregation Stages

Public upstream: 57. Supported: 21. Unsupported: 36.

### Supported Public

- `$addFields`
- `$bucket`
- `$bucketAuto`
- `$count`
- `$documents`
- `$facet`
- `$group`
- `$limit`
- `$lookup`
- `$match`
- `$project`
- `$replaceRoot`
- `$replaceWith`
- `$sample`
- `$set`
- `$skip`
- `$sort`
- `$sortByCount`
- `$unionWith`
- `$unset`
- `$unwind`

### Unsupported Public

- `$changeStream`
- `$changeStreamSplitLargeEvent`
- `$collStats`
- `$currentOp`
- `$densify`
- `$fill`
- `$geoNear`
- `$graphLookup`
- `$indexStats`
- `$listCachedAndActiveUsers`
- `$listCatalog`
- `$listClusterCatalog`
- `$listExtensions` (feature-flagged)
- `$listLocalSessions`
- `$listMqlEntities`
- `$listSampledQueries`
- `$listSearchIndexes`
- `$listSessions`
- `$merge`
- `$out`
- `$planCacheStats`
- `$querySettings`
- `$queryStats` (feature-flagged)
- `$queue`
- `$rankFusion`
- `$redact`
- `$score`
- `$scoreFusion` (feature-flagged)
- `$search`
- `$searchBeta`
- `$searchMeta`
- `$setMetadata`
- `$setVariableFromSubPipeline`
- `$setWindowFields`
- `$shardedDataDistribution`
- `$vectorSearch`

### Internal Or Server-Only Upstream

- `$_internalAllCollectionStats`
- `$_internalApplyOplogUpdate`
- `$_internalBoundedSort`
- `$_internalChangeStreamAddPostImage`
- `$_internalChangeStreamAddPreImage`
- `$_internalChangeStreamCheckInvalidate`
- `$_internalChangeStreamCheckResumability`
- `$_internalChangeStreamCheckTopologyChange`
- `$_internalChangeStreamHandleTopologyChange`
- `$_internalChangeStreamInjectControlEvents`
- `$_internalChangeStreamOplogMatch`
- `$_internalChangeStreamTransform`
- `$_internalChangeStreamUnwindTransaction`
- `$_internalComputeGeoNearDistance`
- `$_internalConvertBucketIndexStats`
- `$_internalDensify`
- `$_internalFindAndModifyImageLookup`
- `$_internalInhibitOptimization`
- `$_internalListCollections`
- `$_internalSearchIdLookup`
- `$_internalSetWindowFields`
- `$_internalShardServerInfo`
- `$_internalShredDocuments`
- `$_internalSplitPipeline`
- `$_internalStreamingGroup`
- `$_internalUnpackBucket`
- `$_unpackBucket`

## Aggregation Expression Operators

Public upstream: 176. Supported: 11. Unsupported: 165.

### Supported Public

- `$and`
- `$eq`
- `$gt`
- `$gte`
- `$in`
- `$literal`
- `$lt`
- `$lte`
- `$ne`
- `$not`
- `$or`

### Unsupported Public

- `$abs`
- `$acos`
- `$acosh`
- `$add`
- `$allElementsTrue`
- `$anyElementTrue`
- `$arrayElemAt`
- `$arrayToObject`
- `$asin`
- `$asinh`
- `$atan`
- `$atan2`
- `$atanh`
- `$avg`
- `$binarySize`
- `$bitAnd`
- `$bitNot`
- `$bitOr`
- `$bitXor`
- `$bottom` (feature-flagged)
- `$bottomN` (feature-flagged)
- `$bsonSize`
- `$ceil`
- `$cmp`
- `$concat`
- `$concatArrays`
- `$cond`
- `$const`
- `$convert`
- `$cos`
- `$cosh`
- `$createObjectId` (feature-flagged)
- `$createUUID` (feature-flagged)
- `$currentDate` (feature-flagged)
- `$dateAdd`
- `$dateDiff`
- `$dateFromParts`
- `$dateFromString`
- `$dateSubtract`
- `$dateToParts`
- `$dateToString`
- `$dateTrunc`
- `$dayOfMonth`
- `$dayOfWeek`
- `$dayOfYear`
- `$degreesToRadians`
- `$deserializeEJSON` (feature-flagged)
- `$divide`
- `$encStrContains` (feature-flagged)
- `$encStrEndsWith` (feature-flagged)
- `$encStrNormalizedEq` (feature-flagged)
- `$encStrStartsWith` (feature-flagged)
- `$exp`
- `$expr`
- `$filter`
- `$first`
- `$firstN`
- `$floor`
- `$function`
- `$getField`
- `$hash` (feature-flagged)
- `$hexHash` (feature-flagged)
- `$hour`
- `$ifNull`
- `$indexOfArray`
- `$indexOfBytes`
- `$indexOfCP`
- `$isArray`
- `$isNumber`
- `$isoDayOfWeek`
- `$isoWeek`
- `$isoWeekYear`
- `$last`
- `$lastN`
- `$let`
- `$ln`
- `$log`
- `$log10`
- `$ltrim`
- `$map`
- `$max`
- `$maxN`
- `$median`
- `$mergeObjects`
- `$meta` (conditional)
- `$millisecond`
- `$min`
- `$minN`
- `$minute`
- `$mod`
- `$month`
- `$multiply`
- `$objectToArray`
- `$percentile`
- `$pow`
- `$radiansToDegrees`
- `$rand`
- `$range`
- `$reduce`
- `$regexFind`
- `$regexFindAll`
- `$regexMatch`
- `$replaceAll`
- `$replaceOne`
- `$reverseArray`
- `$round`
- `$rtrim`
- `$second`
- `$serializeEJSON` (feature-flagged)
- `$setDifference`
- `$setEquals`
- `$setField`
- `$setIntersection`
- `$setIsSubset`
- `$setUnion`
- `$sigmoid` (feature-flagged)
- `$similarityCosine` (feature-flagged)
- `$similarityDotProduct` (feature-flagged)
- `$similarityEuclidean` (feature-flagged)
- `$sin`
- `$sinh`
- `$size`
- `$slice`
- `$sortArray`
- `$split`
- `$sqrt`
- `$stdDevPop`
- `$stdDevSamp`
- `$strLenBytes`
- `$strLenCP`
- `$strcasecmp`
- `$substr`
- `$substrBytes`
- `$substrCP`
- `$subtract`
- `$subtype` (feature-flagged)
- `$sum`
- `$switch`
- `$tan`
- `$tanh`
- `$toArray` (feature-flagged)
- `$toBool`
- `$toDate`
- `$toDecimal`
- `$toDouble`
- `$toHashedIndexKey`
- `$toInt`
- `$toLong`
- `$toLower`
- `$toObject` (feature-flagged)
- `$toObjectId`
- `$toString`
- `$toUUID` (feature-flagged)
- `$toUpper`
- `$top` (feature-flagged)
- `$topN` (feature-flagged)
- `$trim`
- `$trunc`
- `$tsIncrement`
- `$tsSecond`
- `$type`
- `$unsetField`
- `$week`
- `$year`
- `$zip`

### Internal Or Server-Only Upstream

- `$_internalFindAllValuesAtPath`
- `$_internalFleBetween`
- `$_internalFleEq`
- `$_internalIndexKey`
- `$_internalJsEmit`
- `$_internalKeyStringValue`
- `$_internalOwningShard`
- `$_internalSortKey` (conditional)

## Aggregation Accumulators

Public upstream: 25. Supported: 5. Unsupported: 20.

### Supported Public

- `$addToSet`
- `$avg`
- `$first`
- `$push`
- `$sum`

### Unsupported Public

- `$accumulator`
- `$bottom`
- `$bottomN`
- `$concatArrays` (feature-flagged)
- `$count`
- `$firstN`
- `$last`
- `$lastN`
- `$max`
- `$maxN`
- `$median`
- `$mergeObjects`
- `$min`
- `$minN`
- `$percentile`
- `$setUnion` (feature-flagged)
- `$stdDevPop`
- `$stdDevSamp`
- `$top`
- `$topN`

### Internal Or Server-Only Upstream

- `$_internalConstructStats`
- `$_internalJsReduce`

## Aggregation Window Functions

Public upstream: 36. Supported: 0. Unsupported: 36.

### Supported Public

- None

### Unsupported Public

- `$addToSet`
- `$avg`
- `$bottom`
- `$bottomN`
- `$concatArrays` (feature-flagged)
- `$count`
- `$covariancePop`
- `$covarianceSamp`
- `$denseRank`
- `$derivative`
- `$documentNumber`
- `$expMovingAvg`
- `$first`
- `$firstN`
- `$integral`
- `$last`
- `$lastN`
- `$linearFill`
- `$locf`
- `$max`
- `$maxN`
- `$median`
- `$mergeObjects`
- `$min`
- `$minMaxScaler` (feature-flagged)
- `$minN`
- `$percentile`
- `$push`
- `$rank`
- `$setUnion` (feature-flagged)
- `$shift`
- `$stdDevPop`
- `$stdDevSamp`
- `$sum`
- `$top`
- `$topN`

### Internal Or Server-Only Upstream

- None

