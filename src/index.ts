export * from "@aws-sdk/client-dynamodb";
export type * from "@aws-sdk/client-dynamodb";
export type * from "./types";

export * from "./constants";

export { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

// client, utils, constants
export { setClient, ddbClientInstance, getDdbClient } from "./client";
export { consoleError, consoleLog, chunkList, batchWriteTable } from "./utils";
export { DEFAULT_MARSHALL_OPTIONS } from "./constants";

// write, batch write
export { putTableRow, delTableRow } from "./write";
export { batchDelTable, batchPutTable } from "./batchWrite";

// get, query
export { getTableRow } from "./get";
export { queryTableIndex, queryTable } from "./query";

// shallow update, update
export { shallowUpdateTableRow } from "./shallowUpdate";
export { updateTableRow } from "./update";
