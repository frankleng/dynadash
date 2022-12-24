import { getBatchWriteRequest } from "./utils";

export const batchPutTable = getBatchWriteRequest("PutRequest");
export const batchDelTable = getBatchWriteRequest("DeleteRequest");
