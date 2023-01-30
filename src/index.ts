import * as sharedIniFileLoader from "@aws-sdk/shared-ini-file-loader";
import type { SharedConfigFiles } from "@aws-sdk/types";
Object.assign(sharedIniFileLoader, {
  loadSharedConfigFiles: async (): Promise<SharedConfigFiles> => ({
    configFile: {},
    credentialsFile: {},
  }),
});

export * from "@aws-sdk/client-dynamodb";

export * from "./constants";

export { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

// types
export type { QueryOutput, QueryInput, BatchWriteItemOutput, BatchGetItemOutput } from "@aws-sdk/client-dynamodb";
export type { KeyCondMap, KeyCondExpressionMap, FilterExpressionMap } from "./types";

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
