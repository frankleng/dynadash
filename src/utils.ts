import {
  BatchWriteItemCommand,
  BatchWriteItemCommandOutput,
  BatchWriteItemInput,
  QueryCommandInput,
  WriteRequest,
} from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { inspect } from "util";
import { getDdbClient } from "./client";
import { BATCH_WRITE_RETRY_THRESHOLD, DEFAULT_MARSHALL_OPTIONS } from "./constants";
import { FilterExpressionMap, KeyCondExpressionMap } from "./types";

/**
 * @param list
 * @param size
 */
export function chunkList<T>(list: T[], size: number): T[][] {
  return list.reduce((acc: T[][], _: T, i: number) => {
    if (i % size === 0) acc.push(list.slice(i, i + size));
    return acc;
  }, []);
}

export function consoleLog(obj: unknown): void {
  console.log(inspect(obj, false, null, false));
}

export function consoleError(obj: unknown): void {
  console.error(inspect(obj, false, null, false));
}

export function getExpressionFromMap(type: "FilterExpression" | "KeyConditionExpression") {
  return (map: KeyCondExpressionMap | FilterExpressionMap) => {
    const keyCondExpList = [];
    const ExpressionAttributeValues: { [key: string]: string | number } = {};
    const ExpressionAttributeNames: { [key: string]: string } = {};
    try {
      for (const key in map) {
        if (map.hasOwnProperty(key)) {
          const v = map[key];
          const attribute = `#${key}`;
          const anchor = `:${key}`;
          ExpressionAttributeNames[attribute] = key;
          if (typeof v === "undefined") {
            throw Error("Value must not be undefined in an expression.");
          }

          if (typeof v === "string" || typeof v === "number") {
            keyCondExpList.push(`${attribute} = ${anchor}`);
            ExpressionAttributeValues[anchor] = v;
          } else {
            keyCondExpList.push(`${attribute} ${v.op} ${anchor}`);
            ExpressionAttributeValues[anchor] = v.value;
          }
        }
      }
      return {
        [type]: keyCondExpList.join(" and "),
        ExpressionAttributeValues: marshall(ExpressionAttributeValues, {
          ...DEFAULT_MARSHALL_OPTIONS,
        }),
        ExpressionAttributeNames,
      };
    } catch (e) {
      consoleLog(map);
      throw e;
    }
  };
}

export const getKeyCondExpressionFromMap = (map: KeyCondExpressionMap) =>
  getExpressionFromMap("KeyConditionExpression")(map) as {
    ExpressionAttributeValues: QueryCommandInput["ExpressionAttributeValues"];
    KeyConditionExpression: QueryCommandInput["KeyConditionExpression"];
    ExpressionAttributeNames: QueryCommandInput["ExpressionAttributeNames"];
  };
export const getFilterExpressionFromMap = (map: FilterExpressionMap) =>
  getExpressionFromMap("FilterExpression")(map) as {
    ExpressionAttributeValues: QueryCommandInput["ExpressionAttributeValues"];
    FilterExpression: QueryCommandInput["FilterExpression"];
    ExpressionAttributeNames: QueryCommandInput["ExpressionAttributeNames"];
  };

/**
 * Batch write items with exponential backoff
 * When DDB exhausts provisioned write capacity, request items are throttled and returned as UnprocessedItems
 * @param RequestItems
 * @param retryCount
 */
export async function batchWriteTable(
  RequestItems: BatchWriteItemInput["RequestItems"],
  retryCount = 0,
): Promise<BatchWriteItemCommandOutput | null> {
  const query: BatchWriteItemInput = {
    RequestItems,
  };
  const client = getDdbClient();
  try {
    let result: BatchWriteItemCommandOutput | null = await client.send(new BatchWriteItemCommand(query));
    if (
      retryCount < BATCH_WRITE_RETRY_THRESHOLD &&
      result.UnprocessedItems &&
      Object.keys(result.UnprocessedItems).length
    ) {
      // delay between 2 seconds + exponential backoff (max backoff ~4 min, to be safe within 15min Lambda exec timeout)
      const delay = Math.floor(2000 + Math.pow(12, retryCount));
      await new Promise((resolve) => setTimeout(resolve, delay));
      result = await batchWriteTable(result.UnprocessedItems, retryCount + 1);
    }
    if (
      retryCount > BATCH_WRITE_RETRY_THRESHOLD &&
      result?.UnprocessedItems &&
      Object.keys(result.UnprocessedItems).length
    ) {
      console.log("Unprocessed Items:", result.UnprocessedItems);
      throw `Batch Write failed to ${process.env.ORDER_SUMMARY_TABLE_NAME}`;
    }
    return result;
  } catch (e) {
    consoleError(e);
    consoleLog({ query });
    throw e;
  }
}

/**
 * @param request
 */
export function getBatchWriteRequest(request: "PutRequest" | "DeleteRequest") {
  return async function batchWrite<Result, SourceList>(
    TableName: string,
    unmarshalledList: SourceList[],
    predicate?: (item: SourceList) => Result | undefined | Promise<Result | undefined>,
  ): Promise<{ results: (BatchWriteItemCommandOutput | null)[]; actualList: Result[] }> {
    const results = [];
    const actualList: Result[] = [];

    // AWS SDK limits batch requests to 25 - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    // so we have to chunk the list, and create separate requests
    const chunkedList = chunkList(unmarshalledList, 25);

    console.info({ TableName });
    console.info("list length", unmarshalledList.length);
    console.info("# of chunks", chunkedList.length);

    for (const chunk of chunkedList) {
      const requests: WriteRequest[] = [];
      for (const item of chunk) {
        let row: Result = item as unknown as Result;
        if (predicate) {
          try {
            if (predicate.constructor.name === "AsyncFunction") {
              row = await (predicate(item) as Promise<Result>);
            } else {
              row = predicate(item) as Result;
            }
          } catch (e) {
            consoleError(e);
            console.log("Predicate Failure - ");
            consoleLog({ item, row });
            throw e;
          }

          // skip to next if row is falsey
          if (!row) continue;
        }

        const marshallOptions = {
          ...DEFAULT_MARSHALL_OPTIONS,
        };
        try {
          actualList.push(row);
          const marshalledRow = marshall(row, marshallOptions);
          if (request === "DeleteRequest") {
            requests.push({
              DeleteRequest: {
                Key: marshalledRow,
              },
            });
          }
          if (request === "PutRequest") {
            requests.push({
              PutRequest: {
                Item: marshalledRow,
              },
            });
          }
        } catch (e) {
          consoleError(e);
          console.log("Marshall Failure - ");
          consoleLog({ item, row, marshallOptions });
          throw e;
        }
      }

      if (requests.length > 0) {
        const putRequests: BatchWriteItemInput["RequestItems"] = {
          [TableName]: requests,
        };
        const result = await batchWriteTable(putRequests);
        results.push(result);
      }
    }
    return { results, actualList };
  };
}

export function getQueryExpression(
  query: QueryCommandInput,
  params: Partial<Omit<QueryCommandInput, "TableName" | "IndexName">> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
) {
  let result = { ...query };

  const { keyCondExpressionMap, filterExpressionMap, ...rest } = params;
  if (rest) result = { ...result, ...rest };
  if (keyCondExpressionMap) {
    const { KeyConditionExpression, ExpressionAttributeValues, ExpressionAttributeNames } =
      getKeyCondExpressionFromMap(keyCondExpressionMap);
    result["KeyConditionExpression"] = KeyConditionExpression;
    result["ExpressionAttributeNames"] = result["ExpressionAttributeNames"]
      ? {
          ...result["ExpressionAttributeNames"],
          ...ExpressionAttributeNames,
        }
      : ExpressionAttributeNames;
    result["ExpressionAttributeValues"] = result["ExpressionAttributeValues"]
      ? {
          ...result["ExpressionAttributeValues"],
          ...ExpressionAttributeValues,
        }
      : ExpressionAttributeValues;
  }
  if (filterExpressionMap) {
    const { FilterExpression, ExpressionAttributeValues, ExpressionAttributeNames } =
      getFilterExpressionFromMap(filterExpressionMap);
    result["FilterExpression"] = FilterExpression;
    result["ExpressionAttributeNames"] = result["ExpressionAttributeNames"]
      ? {
          ...result["ExpressionAttributeNames"],
          ...ExpressionAttributeNames,
        }
      : ExpressionAttributeNames;
    result["ExpressionAttributeValues"] = result["ExpressionAttributeValues"]
      ? {
          ...result["ExpressionAttributeValues"],
          ...ExpressionAttributeValues,
        }
      : ExpressionAttributeValues;
  }

  return result;
}
