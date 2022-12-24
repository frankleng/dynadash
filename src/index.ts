export * from "@aws-sdk/client-dynamodb";

export * from "./constants";

export { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

export type { QueryOutput, QueryInput, BatchWriteItemOutput, BatchGetItemOutput } from "@aws-sdk/client-dynamodb";

export type { KeyCondMap, KeyCondExpressionMap, FilterExpressionMap } from "./types";

export { setClient, ddbClientInstance, getDdbClient } from "./client";

export { queryTableIndex, queryTable } from "./query";

export { putTableRow, delTableRow } from "./write";

export { batchDelTable, batchPutTable } from "./batchWrite";

export { getTableRow } from "./get";

export { consoleError, consoleLog, chunkList, batchWriteTable } from "./utils";

export { DEFAULT_MARSHALL_OPTIONS } from "./constants";

export { shallowUpdateTableRow } from "./shallowUpdate";
