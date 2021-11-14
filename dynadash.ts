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
  QueryOutput,
  GetItemCommandOutput,
  WriteRequest,
  QueryCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

export const BATCH_WRITE_RETRY_THRESHOLD = 10;

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

/**
 * @param table
 */
export function logTableNameUndefined(table = ''): void {
  console.error('Table name is undefined. ', table);
  console.log(__filename);
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
export async function getTableRow<R>(
  TableName: GetItemInput['TableName'],
  keys: { [x: string]: any },
  projection?: string[],
  consistent?: boolean,
): Promise<(GetItemCommandOutput & { toJs: () => R | null }) | void | null> {
  if (!TableName) return logTableNameUndefined();
  try {
    const ddb = new DynamoDBClient({});
    const query: GetItemInput = {
      TableName,
      Key: marshall(keys),
    };
    if (projection) query['ProjectionExpression'] = projection.join(',');
    if (typeof consistent !== 'undefined') query['ConsistentRead'] = consistent;
    const result = await ddb.send(new GetItemCommand(query));
    return { ...result, toJs: () => (result.Item ? (unmarshall(result.Item) as R) : null) };
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
  const ddb = new DynamoDBClient({});
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
  return async function <R>(
    TableName: PutItemInput['TableName'],
    unmarshalledList: any[],
    predicate?: (item: any) => any,
  ): Promise<{ results: (BatchWriteItemCommandOutput | null)[]; actualList: R[] } | void> {
    if (!TableName) return logTableNameUndefined();
    const results = [];
    const actualList: R[] = [];

    // AWS SDK limits batch requests to 25 - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    // so we have to chunk the list, and create separate requests
    const chunkedList = chunkList(unmarshalledList, 25);

    console.info({ TableName });
    console.info('list length', unmarshalledList.length);
    console.info('# of chunks', chunkedList.length);

    for (const chunk of chunkedList) {
      const items = chunk
        .map((item) => {
          const row: R = predicate ? predicate(item) : item;
          if (!row) return undefined;
          actualList.push(row);
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
          return undefined;
        })
        .filter(Boolean);

      if (items.length > 0) {
        const putRequests: BatchWriteItemInput['RequestItems'] = {
          [TableName]: items as WriteRequest[],
        };
        const result = await batchWriteTable(putRequests);
        results.push(result);
      }
    }
    return { results, actualList };
  };
}

export const batchPutTable = getBatchWriteRequest('PutRequest');
export const batchDelTable = getBatchWriteRequest('DeleteRequest');

/**
 * @param request
 */
function getWriteRequest(request: 'PutItem' | 'DeleteItem') {
  return async function <R>(TableName: PutItemInput['TableName'], data: R) {
    const client = new DynamoDBClient({});
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

function getQueryExpression(
  query: QueryCommandInput,
  params: Partial<Omit<QueryCommandInput, 'TableName' | 'IndexName'>> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
) {
  let result = { ...query };

  const { keyCondExpressionMap, filterExpressionMap, ...rest } = params;
  if (rest) result = { ...result, ...rest };
  if (keyCondExpressionMap) {
    const { KeyConditionExpression, ExpressionAttributeValues, ExpressionAttributeNames } = getKeyCondExpressionFromMap(
      keyCondExpressionMap,
    );
    result['KeyConditionExpression'] = KeyConditionExpression;
    result['ExpressionAttributeNames'] = result['ExpressionAttributeNames']
      ? {
          ...result['ExpressionAttributeNames'],
          ...ExpressionAttributeNames,
        }
      : ExpressionAttributeNames;
    result['ExpressionAttributeValues'] = result['ExpressionAttributeValues']
      ? {
          ...result['ExpressionAttributeValues'],
          ...ExpressionAttributeValues,
        }
      : ExpressionAttributeValues;
  }
  if (filterExpressionMap) {
    const { FilterExpression, ExpressionAttributeValues, ExpressionAttributeNames } = getFilterExpressionFromMap(
      filterExpressionMap,
    );
    result['FilterExpression'] = FilterExpression;
    result['ExpressionAttributeNames'] = result['ExpressionAttributeNames']
      ? {
          ...result['ExpressionAttributeNames'],
          ...ExpressionAttributeNames,
        }
      : ExpressionAttributeNames;
    result['ExpressionAttributeValues'] = result['ExpressionAttributeValues']
      ? {
          ...result['ExpressionAttributeValues'],
          ...ExpressionAttributeValues,
        }
      : ExpressionAttributeValues;
  }

  return result;
}

async function handleQueryCommand<R>(query: QueryCommandInput): Promise<(QueryOutput & { toJs: () => R[] }) | null> {
  try {
    const client = new DynamoDBClient({});
    let result = await client.send(new QueryCommand(query));

    if (result?.LastEvaluatedKey) {
      let items = result.Items || [];
      while (result.LastEvaluatedKey) {
        result = await client.send(new QueryCommand({ ...query, ExclusiveStartKey: result.LastEvaluatedKey }));
        items = items.concat(result.Items || []);
      }
      result.Items = items;
    }

    return {
      ...result,
      toJs: () => (result.Items?.length ? result.Items.map((row) => unmarshall(row) as R) : []),
    };
  } catch (e) {
    console.error(e);
    return null;
  }
}

/**
 * @param TableName
 * @param IndexName
 * @param params
 */
export async function queryTableIndex<R>(
  TableName: QueryCommandInput['TableName'],
  IndexName: QueryCommandInput['IndexName'],
  params?: Partial<Omit<QueryCommandInput, 'TableName' | 'IndexName'>> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
) {
  const query: QueryCommandInput = params
    ? getQueryExpression(
        {
          TableName,
          IndexName,
        },
        params,
      )
    : {
        TableName,
        IndexName,
      };

  return handleQueryCommand<R>(query);
}

/**
 * @param TableName
 * @param params
 */
export async function queryTable<R>(
  TableName: QueryCommandInput['TableName'],
  params?: Partial<Omit<QueryCommandInput, 'TableName' | 'IndexName'>> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
) {
  const query: QueryCommandInput = params
    ? getQueryExpression(
        {
          TableName,
        },
        params,
      )
    : {
        TableName,
      };

  return handleQueryCommand<R>(query);
}

export async function updateTableRow<R>(
  TableName: UpdateItemCommandInput['TableName'],
  keys: { [x: string]: any },
  UpdateExpression: string,
  expressionAttributeValues: { [x: string]: any },
  ExpressionAttributeNames?: { [x: string]: string },
  ReturnValues = 'ALL_NEW',
) {
  if (!TableName) return logTableNameUndefined();
  try {
    const ddb = new DynamoDBClient({});
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
    return { ...result, toJs: () => (result.Attributes ? (unmarshall(result.Attributes) as R) : {}) };
  } catch (e) {
    console.error(e);
    return null;
  }
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
