import {
  DynamoDBClient,
  GetItemCommand,
  BatchWriteItemInput,
  GetItemInput,
  BatchWriteItemCommand,
  BatchWriteItemCommandOutput,
  PutItemCommand,
  PutItemInput,
  DeleteItemCommand,
  QueryCommandInput,
  QueryCommand,
  UpdateItemCommand,
  UpdateItemCommandInput,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { captureAWSv3Client } from 'aws-xray-sdk-core';

export const BATCH_WRITE_RETRY_THRESHOLD = 10;

/**
 * @param list
 * @param size
 */
export function chunkList(list: any[], size: number) {
  return list.reduce((acc: any[], _: any, i: number) => {
    if (i % size === 0) acc.push(list.slice(i, i + size));
    return acc;
  }, []);
}

/**
 * @param table
 */
export function logTableNameUndefined(table = '') {
  console.error('Table name is undefined. ', table);
  console.trace();
}

export type KeyCondMap = { op: '=' | '>' | '<' | '>=' | '<='; value: string | number };
export type KeyCondExpressionMap = {
  [key: string]: string | number | KeyCondMap;
};
export type FilterExpressionMap = {
  [key: string]:
    | string
    | number
    | (
        | KeyCondMap
        | {
            op: KeyCondMap['op'] & { op: '<>' };
            value: KeyCondMap['value'];
          }
      );
};
function getExpressionFromMap(type: 'FilterExpression' | 'KeyConditionExpression') {
  return (map: KeyCondExpressionMap | FilterExpressionMap) => {
    const keyCondExpList = [];
    const ExpressionAttributeValues: { [key: string]: string | number } = {};
    const ExpressionAttributeNames: { [key: string]: string } = {};
    for (const key in map) {
      if (map.hasOwnProperty(key)) {
        const v = map[key];
        const attribute = `#${key}`;
        const anchor = `:${key}`;
        ExpressionAttributeNames[attribute] = key;
        if (typeof v === 'string' || typeof v === 'number') {
          keyCondExpList.push(`${attribute} = ${anchor}`);
          ExpressionAttributeValues[anchor] = v;
        } else {
          keyCondExpList.push(`${attribute} ${v.op} ${anchor}`);
          ExpressionAttributeValues[anchor] = v.value;
        }
      }
    }
    return {
      [type]: keyCondExpList.join(' and '),
      ExpressionAttributeValues: marshall(ExpressionAttributeValues, {
        removeUndefinedValues: true,
      }),
      ExpressionAttributeNames,
    };
  };
}

export const getKeyCondExpressionFromMap = (map: KeyCondExpressionMap) =>
  getExpressionFromMap('KeyConditionExpression')(map) as {
    ExpressionAttributeValues: QueryCommandInput['ExpressionAttributeValues'];
    KeyConditionExpression: QueryCommandInput['KeyConditionExpression'];
    ExpressionAttributeNames: QueryCommandInput['ExpressionAttributeNames'];
  };
export const getFilterExpressionFromMap = (map: FilterExpressionMap) =>
  getExpressionFromMap('FilterExpression')(map) as {
    ExpressionAttributeValues: QueryCommandInput['ExpressionAttributeValues'];
    FilterExpression: QueryCommandInput['FilterExpression'];
    ExpressionAttributeNames: QueryCommandInput['ExpressionAttributeNames'];
  };

/**
 * @param TableName
 * @param keys
 * @param projection
 * @param consistent
 */
export async function getTableRow(
  TableName: GetItemInput['TableName'],
  keys: { [x: string]: any },
  projection?: string[],
  consistent?: boolean,
) {
  if (!TableName) return logTableNameUndefined();
  try {
    const ddb = captureAWSv3Client(new DynamoDBClient({}));
    const query: GetItemInput = {
      TableName,
      Key: marshall(keys),
    };
    if (projection) query['ProjectionExpression'] = projection.join(',');
    if (typeof consistent !== 'undefined') query['ConsistentRead'] = consistent;
    const result = await ddb.send(new GetItemCommand(query));
    return result.Item ? unmarshall(result.Item) : null;
  } catch (e) {
    console.error(e);
    return null;
  }
}

/**
 * Batch write items with exponential backoff
 * When DDB exhausts provisioned write capacity, request items are throttled and returned as UnprocessedItems
 * @param RequestItems
 * @param retryCount
 */
async function batchWriteTable(
  RequestItems: BatchWriteItemInput['RequestItems'],
  retryCount = 0,
): Promise<BatchWriteItemCommandOutput | null> {
  const ddb = captureAWSv3Client(new DynamoDBClient({}));
  const query: BatchWriteItemInput = {
    RequestItems,
  };
  let result: BatchWriteItemCommandOutput | null = await ddb.send(new BatchWriteItemCommand(query));

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
    console.log('Unprocessed Items:', result.UnprocessedItems);
    throw `Batch Write failed to ${process.env.ORDER_SUMMARY_TABLE_NAME}`;
  }
  return result;
}

/**
 * @param request
 */
function getBatchWriteRequest(request: 'PutRequest' | 'DeleteRequest') {
  return async function (
    TableName: PutItemInput['TableName'],
    unmarshalledList: any[],
    predicate?: (item: any) => any,
  ) {
    if (!TableName) return logTableNameUndefined();
    const results = [];

    // AWS SDK limits batch requests to 25 - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    // so we have to chunk the list, and create separate requests
    const chunkedList = chunkList(unmarshalledList, 25);

    console.log('list length', unmarshalledList.length);
    console.log('# of chunks', chunkedList.length);

    for (const chunk of chunkedList) {
      console.log('chunk length', chunk.length);
      const putRequests: BatchWriteItemInput['RequestItems'] = {
        [TableName]: chunk.map((item: any) => {
          const row = predicate ? predicate(item) : item;
          const marshalledRow = marshall(row, {
            removeUndefinedValues: true,
          });
          if (request === 'DeleteRequest') {
            return {
              DeleteRequest: {
                Key: marshalledRow,
              },
            };
          }
          if (request === 'PutRequest') {
            return {
              PutRequest: {
                Item: marshalledRow,
              },
            };
          }
          return {};
        }),
      };
      const result = await batchWriteTable(putRequests);
      results.push(result);
    }
    return results;
  };
}

export const batchPutTable = getBatchWriteRequest('PutRequest');
export const batchDelTable = getBatchWriteRequest('DeleteRequest');

/**
 * @param request
 */
function getWriteRequest(request: 'PutItem' | 'DeleteItem') {
  return async function (TableName: PutItemInput['TableName'], data: any) {
    const client = captureAWSv3Client(new DynamoDBClient({}));
    if (request === 'PutItem') {
      return client.send(
        new PutItemCommand({
          TableName,
          Item: marshall(data, { removeUndefinedValues: true }),
        }),
      );
    }
    if (request === 'DeleteItem') {
      return client.send(
        new DeleteItemCommand({
          TableName,
          Key: marshall(data, { removeUndefinedValues: true }),
        }),
      );
    }
    return null;
  };
}

export const putTableRow = getWriteRequest('PutItem');
export const delTableRow = getWriteRequest('DeleteItem');

/**
 * @param TableName
 * @param IndexName
 * @param Limit
 * @param params
 */
export async function queryTableIndex(
  TableName: QueryCommandInput['TableName'],
  IndexName: QueryCommandInput['IndexName'],
  params?: Partial<Omit<QueryCommandInput, 'TableName' | 'IndexName'>> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
) {
  try {
    const client = captureAWSv3Client(new DynamoDBClient({}));
    let query: QueryCommandInput = {
      TableName,
      IndexName,
    };
    if (params) {
      const { keyCondExpressionMap, filterExpressionMap, ...rest } = params;
      if (rest) query = { ...query, ...rest };
      if (keyCondExpressionMap) {
        const {
          KeyConditionExpression,
          ExpressionAttributeValues,
          ExpressionAttributeNames,
        } = getKeyCondExpressionFromMap(keyCondExpressionMap);
        query['KeyConditionExpression'] = KeyConditionExpression;
        query['ExpressionAttributeNames'] = query['ExpressionAttributeNames']
          ? {
              ...query['ExpressionAttributeNames'],
              ...ExpressionAttributeNames,
            }
          : ExpressionAttributeNames;
        query['ExpressionAttributeValues'] = query['ExpressionAttributeValues']
          ? {
              ...query['ExpressionAttributeValues'],
              ...ExpressionAttributeValues,
            }
          : ExpressionAttributeValues;
      }
      if (filterExpressionMap) {
        const { FilterExpression, ExpressionAttributeValues, ExpressionAttributeNames } = getFilterExpressionFromMap(
          filterExpressionMap,
        );
        query['FilterExpression'] = FilterExpression;
        query['ExpressionAttributeNames'] = query['ExpressionAttributeNames']
          ? {
              ...query['ExpressionAttributeNames'],
              ...ExpressionAttributeNames,
            }
          : ExpressionAttributeNames;
        query['ExpressionAttributeValues'] = query['ExpressionAttributeValues']
          ? {
              ...query['ExpressionAttributeValues'],
              ...ExpressionAttributeValues,
            }
          : ExpressionAttributeValues;
      }
    }
    const { Items, ...rest } = await client.send(new QueryCommand(query));
    return { list: Items?.length ? Items.map((row) => unmarshall(row)) : [], ...rest };
  } catch (e) {
    console.error(e);
    return null;
  }
}

export async function updateTableRow(
  TableName: UpdateItemCommandInput['TableName'],
  keys: { [x: string]: any },
  UpdateExpression: string,
  expressionAttributeValues: { [x: string]: any },
  ExpressionAttributeNames?: { [x: string]: string },
  ReturnValues = 'ALL_NEW',
) {
  if (!TableName) return logTableNameUndefined();
  const ddb = captureAWSv3Client(new DynamoDBClient({}));
  const query: UpdateItemCommandInput = {
    TableName,
    Key: marshall(keys),
    UpdateExpression,
    ExpressionAttributeValues: marshall(expressionAttributeValues, {
      removeUndefinedValues: true,
    }),
    ExpressionAttributeNames,
    ReturnValues,
  };
  const result = await ddb.send(new UpdateItemCommand(query));
  return result ? result : null;
}

/**
 * Update attributes directly without path support, use `updateTableRow` for deep updates
 * @param TableName
 * @param keys
 * @param row
 */
export async function shallowUpdateTableRow(
  TableName: UpdateItemCommandInput['TableName'],
  keys: { [x: string]: any },
  row: { [x: string]: any },
) {
  const updateExpressions = [];
  const expressionAttributeValues: {
    [x: string]: any;
  } = {};
  const expressionAttributeNames: { [x: string]: string } = {};

  for (const key in row) {
    if (row.hasOwnProperty(key)) {
      const val = row[key];
      updateExpressions.push(`#${key} = :${key}`);
      expressionAttributeValues[`:${key}`] = val;
      expressionAttributeNames[`#${key}`] = key;
    }
  }

  return updateTableRow(
    TableName,
    keys,
    `SET ${updateExpressions.join(', ')}`,
    expressionAttributeValues,
    expressionAttributeNames,
  );
}
