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
| Aggregation expression operators | 176 | 146 | 6 | 24 |
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

Public upstream: 176. Supported: 146. Unsupported: 6. Ignored: 24.

### Supported Public

- `$abs`
- `$acos`
- `$acosh`
- `$add`
- `$allElementsTrue`
- `$and`
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
- `$divide`
- `$eq`
- `$exp`
- `$expr`
- `$filter`
- `$first`
- `$firstN`
- `$floor`
- `$getField`
- `$gt`
- `$gte`
- `$hour`
- `$ifNull`
- `$in`
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
- `$literal`
- `$ln`
- `$log`
- `$log10`
- `$lt`
- `$lte`
- `$ltrim`
- `$map`
- `$max`
- `$maxN`
- `$mergeObjects`
- `$millisecond`
- `$min`
- `$minN`
- `$minute`
- `$mod`
- `$month`
- `$multiply`
- `$ne`
- `$not`
- `$objectToArray`
- `$or`
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
- `$setDifference`
- `$setEquals`
- `$setField`
- `$setIntersection`
- `$setIsSubset`
- `$setUnion`
- `$sin`
- `$sinh`
- `$size`
- `$slice`
- `$sortArray`
- `$split`
- `$sqrt`
- `$strLenBytes`
- `$strLenCP`
- `$strcasecmp`
- `$substr`
- `$substrBytes`
- `$substrCP`
- `$subtract`
- `$sum`
- `$switch`
- `$tan`
- `$tanh`
- `$toBool`
- `$toDate`
- `$toDecimal`
- `$toDouble`
- `$toInt`
- `$toLong`
- `$toLower`
- `$toObjectId`
- `$toString`
- `$toUpper`
- `$trim`
- `$trunc`
- `$tsIncrement`
- `$tsSecond`
- `$type`
- `$unsetField`
- `$week`
- `$year`
- `$zip`

### Unsupported Public

- `$median`
- `$meta` (conditional)
- `$percentile`
- `$stdDevPop`
- `$stdDevSamp`
- `$toHashedIndexKey`

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
- `$function` (ignored)
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

