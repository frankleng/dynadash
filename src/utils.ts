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
import { ConditionExpressionMap, FilterExpressionMap, KeyCondExpressionMap } from "./types";

const MAX_BATCH_WRITE_SIZE = 25;

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
            if (v.op === "BETWEEN") {
              const lowVal = v.low;
              const highVal = v.high;
              const lowAnchor = `:${key}_low`;
              const highAnchor = `:${key}_high`;
              keyCondExpList.push(`${attribute} BETWEEN ${lowAnchor} AND ${highAnchor}`);
              ExpressionAttributeValues[lowAnchor] = lowVal;
              ExpressionAttributeValues[highAnchor] = highVal;
            } else {
              keyCondExpList.push(`${attribute} ${v.op} ${anchor}`);
              ExpressionAttributeValues[anchor] = v.value;
            }
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
    transformer?: (item: SourceList, i: number) => Result | undefined | Promise<Result | undefined>,
  ): Promise<{ results: (BatchWriteItemCommandOutput | null)[]; actualList: Result[] }> {
    const results = [];
    const actualList: Result[] = [];

    // AWS SDK limits batch requests to 25 - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    // so we have to chunk the list, and create separate requests
    const chunkedList = chunkList(unmarshalledList, MAX_BATCH_WRITE_SIZE);

    console.info({ TableName });
    console.info("list length", unmarshalledList.length);
    console.info("# of chunks", chunkedList.length);

    // only dedupe delete for now
    const dedupeRequests: { [key: string]: true } = {};

    for (const [i, chunk] of chunkedList.entries()) {
      const requests: WriteRequest[] = [];
      const baseCount = i * MAX_BATCH_WRITE_SIZE;
      for (const [j, item] of chunk.entries()) {
        const totalIndex = baseCount + j;
        let row: Result = item as unknown as Result;
        if (transformer) {
          try {
            if (transformer.constructor.name === "AsyncFunction") {
              row = await (transformer(item, totalIndex) as Promise<Result>);
            } else {
              row = transformer(item, totalIndex) as Result;
            }
          } catch (e) {
            consoleError(e);
            console.log("Predicate Failure - ");
            consoleLog({ TableName, item, row });
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
          if (request === "DeleteRequest") {
            const dedupeKey = JSON.stringify(row);
            if (dedupeRequests[dedupeKey]) continue;

            requests.push({
              DeleteRequest: {
                Key: marshall(row, marshallOptions),
              },
            });
            dedupeRequests[dedupeKey] = true;
          }
          if (request === "PutRequest") {
            requests.push({
              PutRequest: {
                Item: marshall(row, marshallOptions),
              },
            });
          }
        } catch (e) {
          consoleError(e);
          console.log("Marshall Failure - ");
          consoleLog({ TableName, item, row, marshallOptions });
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

/**
 *
 * @param row
 * @param condExps
 * @param includeAll - if true, will include all fields in the row, even if they are not in the condExps, updates usually need all fields, write conditions do not
 */
// eslint-disable-next-line @typescript-eslint/ban-types
export function getConditionExpression<T extends {}>(row: T, condExps?: ConditionExpressionMap, includeAll = false) {
  const updateExpressions = [];
  const expressionAttributeValues: {
    [x: string]: any;
  } = {};
  const ExpressionAttributeNames: { [x: string]: string } = {};

  const conditionExpression: string[] = [];

  if (Array.isArray(condExps)) {
    for (const condExp of condExps) {
      const { key, op, value, func, logicOp } = condExp;

      let cond;
      if (op === "IN") {
        const valStr = value
          .map((v, i) => {
            const k = `:${key}IN-${i}`;
            expressionAttributeValues[k] = v;
            return k;
          })
          .join(", ");
        cond = `(#${key} IN (${valStr}))`;
        ExpressionAttributeNames[`#${key}`] = key;
      } else if (op === "BETWEEN") {
        cond = `(#${key} between :${key}Xaa and :${key}Xbb)`;
        expressionAttributeValues[`:${key}Xaa`] = value[0];
        expressionAttributeValues[`:${key}Xbb`] = value[1];
        ExpressionAttributeNames[`#${key}`] = key;
      } else if (typeof op === "undefined" && func) {
        if (!op) {
          cond = `${func}(#${key})`;
          ExpressionAttributeNames[`#${key}`] = key;
        } else {
          cond = `${func}(#${key}) ${op} :${key}Xvv`;
          expressionAttributeValues[`:${key}Xvv`] = value;
          ExpressionAttributeNames[`#${key}`] = key;
        }
      } else {
        cond = `#${key} ${op} :${key}Xvv`;
        ExpressionAttributeNames[`#${key}`] = key;
        expressionAttributeValues[`:${key}Xvv`] = value;
      }

      if (logicOp) cond += ` ${logicOp}`;
      conditionExpression.push(cond);
    }
  } else if (typeof condExps === "string") {
    conditionExpression.push(condExps);
  }

  if (includeAll) {
    for (const key in row) {
      if (row.hasOwnProperty(key)) {
        const val = row[key];
        updateExpressions.push(`#${key} = :${key}`);
        expressionAttributeValues[`:${key}`] = val;
        ExpressionAttributeNames[`#${key}`] = key;
      }
    }
  } else {
    for (const key in row) {
      if (ExpressionAttributeNames[`#${key}`]) {
        updateExpressions.push(`#${key} = :${key}`);
      }
    }
  }

  return {
    ConditionExpression: conditionExpression.join(" ") || undefined,
    UpdateExpression: `SET ${updateExpressions.join(", ")}`,
    expressionAttributeValues:
      Object.keys(expressionAttributeValues).length > 0 ? expressionAttributeValues : undefined,
    ExpressionAttributeNames,
  };
}
