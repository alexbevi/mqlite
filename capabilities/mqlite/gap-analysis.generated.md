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

| Category | Public upstream | Supported | Unsupported | Ignored |
| --- | ---: | ---: | ---: | ---: |
| Query operators | 38 | 29 | 9 | 0 |
| Aggregation stages | 54 | 44 | 0 | 10 |
| Aggregation expression operators | 176 | 68 | 85 | 23 |
| Aggregation accumulators | 25 | 5 | 20 | 0 |
| Aggregation window functions | 36 | 15 | 21 | 0 |

## Query Operators

Public upstream: 38. Supported: 29. Unsupported: 9. Ignored: 0.

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

### Ignored Public

- None

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

Public upstream: 54. Supported: 44. Unsupported: 0. Ignored: 10.

### Supported Public

- `$addFields`
- `$bucket`
- `$bucketAuto`
- `$changeStream`
- `$changeStreamSplitLargeEvent`
- `$collStats`
- `$count`
- `$currentOp`
- `$densify`
- `$documents`
- `$facet`
- `$fill`
- `$geoNear`
- `$graphLookup`
- `$group`
- `$indexStats`
- `$limit`
- `$listCachedAndActiveUsers`
- `$listCatalog`
- `$listClusterCatalog`
- `$listLocalSessions`
- `$listMqlEntities`
- `$listSampledQueries`
- `$listSearchIndexes`
- `$listSessions`
- `$lookup`
- `$match`
- `$merge`
- `$out`
- `$planCacheStats`
- `$project`
- `$querySettings`
- `$redact`
- `$replaceRoot`
- `$replaceWith`
- `$sample`
- `$set`
- `$setWindowFields`
- `$skip`
- `$sort`
- `$sortByCount`
- `$unionWith`
- `$unset`
- `$unwind`

### Unsupported Public

- None

### Ignored Public

- `$listExtensions` (feature-flagged, ignored)
- `$queryStats` (feature-flagged, ignored)
- `$rankFusion` (ignored)
- `$score` (ignored)
- `$scoreFusion` (feature-flagged, ignored)
- `$search` (ignored)
- `$searchBeta` (ignored)
- `$searchMeta` (ignored)
- `$shardedDataDistribution` (ignored)
- `$vectorSearch` (ignored)

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
- `$queue`
- `$setMetadata`
- `$setVariableFromSubPipeline`

## Aggregation Expression Operators

Public upstream: 176. Supported: 68. Unsupported: 85. Ignored: 23.

### Supported Public

- `$abs`
- `$add`
- `$allElementsTrue`
- `$and`
- `$anyElementTrue`
- `$arrayElemAt`
- `$arrayToObject`
- `$bitAnd`
- `$bitNot`
- `$bitOr`
- `$bitXor`
- `$ceil`
- `$cmp`
- `$concat`
- `$concatArrays`
- `$cond`
- `$const`
- `$divide`
- `$eq`
- `$expr`
- `$filter`
- `$first`
- `$floor`
- `$getField`
- `$gt`
- `$gte`
- `$ifNull`
- `$in`
- `$indexOfArray`
- `$indexOfBytes`
- `$indexOfCP`
- `$isArray`
- `$isNumber`
- `$last`
- `$let`
- `$literal`
- `$lt`
- `$lte`
- `$map`
- `$mergeObjects`
- `$mod`
- `$multiply`
- `$ne`
- `$not`
- `$objectToArray`
- `$or`
- `$range`
- `$reduce`
- `$reverseArray`
- `$round`
- `$setDifference`
- `$setEquals`
- `$setField`
- `$setIntersection`
- `$setIsSubset`
- `$setUnion`
- `$size`
- `$slice`
- `$strLenBytes`
- `$strLenCP`
- `$strcasecmp`
- `$subtract`
- `$switch`
- `$toLower`
- `$toUpper`
- `$trunc`
- `$type`
- `$unsetField`

### Unsupported Public

- `$acos`
- `$acosh`
- `$asin`
- `$asinh`
- `$atan`
- `$atan2`
- `$atanh`
- `$avg`
- `$binarySize`
- `$bsonSize`
- `$convert`
- `$cos`
- `$cosh`
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
- `$exp`
- `$firstN`
- `$function`
- `$hour`
- `$isoDayOfWeek`
- `$isoWeek`
- `$isoWeekYear`
- `$lastN`
- `$ln`
- `$log`
- `$log10`
- `$ltrim`
- `$max`
- `$maxN`
- `$median`
- `$meta` (conditional)
- `$millisecond`
- `$min`
- `$minN`
- `$minute`
- `$month`
- `$percentile`
- `$pow`
- `$radiansToDegrees`
- `$rand`
- `$regexFind`
- `$regexFindAll`
- `$regexMatch`
- `$replaceAll`
- `$replaceOne`
- `$rtrim`
- `$second`
- `$sin`
- `$sinh`
- `$sortArray`
- `$split`
- `$sqrt`
- `$stdDevPop`
- `$stdDevSamp`
- `$substr`
- `$substrBytes`
- `$substrCP`
- `$sum`
- `$tan`
- `$tanh`
- `$toBool`
- `$toDate`
- `$toDecimal`
- `$toDouble`
- `$toHashedIndexKey`
- `$toInt`
- `$toLong`
- `$toObjectId`
- `$toString`
- `$trim`
- `$tsIncrement`
- `$tsSecond`
- `$week`
- `$year`
- `$zip`

### Ignored Public

- `$bottom` (feature-flagged, ignored)
- `$bottomN` (feature-flagged, ignored)
- `$createObjectId` (feature-flagged, ignored)
- `$createUUID` (feature-flagged, ignored)
- `$currentDate` (feature-flagged, ignored)
- `$deserializeEJSON` (feature-flagged, ignored)
- `$encStrContains` (feature-flagged, ignored)
- `$encStrEndsWith` (feature-flagged, ignored)
- `$encStrNormalizedEq` (feature-flagged, ignored)
- `$encStrStartsWith` (feature-flagged, ignored)
- `$hash` (feature-flagged, ignored)
- `$hexHash` (feature-flagged, ignored)
- `$serializeEJSON` (feature-flagged, ignored)
- `$sigmoid` (feature-flagged, ignored)
- `$similarityCosine` (feature-flagged, ignored)
- `$similarityDotProduct` (feature-flagged, ignored)
- `$similarityEuclidean` (feature-flagged, ignored)
- `$subtype` (feature-flagged, ignored)
- `$toArray` (feature-flagged, ignored)
- `$toObject` (feature-flagged, ignored)
- `$toUUID` (feature-flagged, ignored)
- `$top` (feature-flagged, ignored)
- `$topN` (feature-flagged, ignored)

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

Public upstream: 25. Supported: 5. Unsupported: 20. Ignored: 0.

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

### Ignored Public

- None

### Internal Or Server-Only Upstream

- `$_internalConstructStats`
- `$_internalJsReduce`

## Aggregation Window Functions

Public upstream: 36. Supported: 15. Unsupported: 21. Ignored: 0.

### Supported Public

- `$addToSet`
- `$avg`
- `$count`
- `$denseRank`
- `$documentNumber`
- `$first`
- `$last`
- `$linearFill`
- `$locf`
- `$max`
- `$min`
- `$push`
- `$rank`
- `$shift`
- `$sum`

### Unsupported Public

- `$bottom`
- `$bottomN`
- `$concatArrays` (feature-flagged)
- `$covariancePop`
- `$covarianceSamp`
- `$derivative`
- `$expMovingAvg`
- `$firstN`
- `$integral`
- `$lastN`
- `$maxN`
- `$median`
- `$mergeObjects`
- `$minMaxScaler` (feature-flagged)
- `$minN`
- `$percentile`
- `$setUnion` (feature-flagged)
- `$stdDevPop`
- `$stdDevSamp`
- `$top`
- `$topN`

### Ignored Public

- None

### Internal Or Server-Only Upstream

- None

