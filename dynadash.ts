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
  UpdateItemCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { inspect } from 'util';

export const BATCH_WRITE_RETRY_THRESHOLD = 10;

export const DEFAULT_MARSHALL_OPTIONS = { removeUndefinedValues: true, convertEmptyValues: true };

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
        ...DEFAULT_MARSHALL_OPTIONS,
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
 * @param params
 */
export async function getTableRow<R>(
  TableName: GetItemInput['TableName'],
  keys: { [x: string]: any },
  params?: Omit<GetItemInput, 'TableName' | 'Key'> & { projection?: string[] },
): Promise<(GetItemCommandOutput & { toJs: () => R | null }) | void | null> {
  const { projection, ...rest } = params || {};

  const query: GetItemInput = {
    TableName,
    Key: marshall(keys),
    ...rest,
  };
  try {
    const ddb = new DynamoDBClient({});

    if (projection) query['ProjectionExpression'] = projection.join(',');
    const result = await ddb.send(new GetItemCommand(query));
    return { ...result, toJs: () => (result.Item ? (unmarshall(result.Item) as R) : null) };
  } catch (e) {
    consoleError(e);
    consoleError({ query });
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
  try {
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
  } catch (e) {
    consoleError(e);
    consoleLog({ query });
    throw e;
  }
}

/**
 * @param request
 */
function getBatchWriteRequest(request: 'PutRequest' | 'DeleteRequest') {
  return async function <R>(
    TableName: string,
    unmarshalledList: any[],
    predicate?: (item: any) => R | undefined | Promise<R | undefined>,
  ): Promise<{ results: (BatchWriteItemCommandOutput | null)[]; actualList: R[] } | void> {
    const results = [];
    const actualList: R[] = [];

    // AWS SDK limits batch requests to 25 - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    // so we have to chunk the list, and create separate requests
    const chunkedList = chunkList(unmarshalledList, 25);

    console.info({ TableName });
    console.info('list length', unmarshalledList.length);
    console.info('# of chunks', chunkedList.length);

    for (const chunk of chunkedList) {
      const requests: WriteRequest[] = [];
      for (const item of chunk) {
        try {
          let row = item;
          if (predicate) {
            if (predicate.constructor.name === 'AsyncFunction') {
              row = await (predicate(item) as Promise<R>);
            } else {
              row = predicate(item);
            }
          }
          // skip to next if row is falsey
          if (!row) continue;

          actualList.push(row);
          const marshalledRow = marshall(row, {
            ...DEFAULT_MARSHALL_OPTIONS,
          });
          if (request === 'DeleteRequest') {
            requests.push({
              DeleteRequest: {
                Key: marshalledRow,
              },
            });
          }
          if (request === 'PutRequest') {
            requests.push({
              PutRequest: {
                Item: marshalledRow,
              },
            });
          }
        } catch (e) {
          consoleError(e);
          consoleLog({ item });
          throw e;
        }
      }

      if (requests.length > 0) {
        const putRequests: BatchWriteItemInput['RequestItems'] = {
          [TableName]: requests,
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
  return async function <R>(TableName: PutItemInput['TableName'], data: Partial<R>) {
    try {
      const client = new DynamoDBClient({});
      if (request === 'PutItem') {
        return client.send(
          new PutItemCommand({
            TableName,
            Item: marshall(data, { ...DEFAULT_MARSHALL_OPTIONS }),
          }),
        );
      }
      if (request === 'DeleteItem') {
        return client.send(
          new DeleteItemCommand({
            TableName,
            Key: marshall(data, { ...DEFAULT_MARSHALL_OPTIONS }),
          }),
        );
      }
      return null;
    } catch (e) {
      consoleError(e);
      consoleError({ TableName, request, data });
      throw e;
    }
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
    const { KeyConditionExpression, ExpressionAttributeValues, ExpressionAttributeNames } =
      getKeyCondExpressionFromMap(keyCondExpressionMap);
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
    const { FilterExpression, ExpressionAttributeValues, ExpressionAttributeNames } =
      getFilterExpressionFromMap(filterExpressionMap);
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

async function handleQueryCommand<R>(
  query: QueryCommandInput,
): Promise<(QueryOutput & { toJs: (predicate?: (row: R) => R) => R[] }) | null> {
  try {
    const client = new DynamoDBClient({});
    let result = await client.send(new QueryCommand(query));

    if (result?.LastEvaluatedKey && !query.ExclusiveStartKey && !query.Limit) {
      let items = result.Items || [];
      while (result.LastEvaluatedKey) {
        result = await client.send(new QueryCommand({ ...query, ExclusiveStartKey: result.LastEvaluatedKey }));
        items = items.concat(result.Items || []);
      }
      result.Items = items;
    }

    return {
      ...result,
      toJs: (predicate) =>
        result.Items?.length
          ? result.Items.map((row) => {
              const result = unmarshall(row) as R;
              return predicate ? predicate(result) : result;
            })
          : [],
    };
  } catch (e) {
    consoleError(e);
    consoleError({ query });
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
  keys: Partial<R>,
  params: {
    UpdateExpression: string;
    expressionAttributeValues: { [x: string]: any };
    ExpressionAttributeNames?: { [x: string]: string };
    ConditionExpression?: string;
  },
  ReturnValues = 'ALL_NEW',
): Promise<(UpdateItemCommandOutput & { toJs: (predicate?: (row: R) => R) => R }) | null> {
  const { UpdateExpression, expressionAttributeValues, ExpressionAttributeNames } = params;
  const query: UpdateItemCommandInput = {
    TableName,
    Key: marshall(keys),
    UpdateExpression,
    ExpressionAttributeValues: marshall(expressionAttributeValues, {
      ...DEFAULT_MARSHALL_OPTIONS,
    }),
    ExpressionAttributeNames,
    ReturnValues,
  };
  try {
    const ddb = new DynamoDBClient({});
    const result = await ddb.send(new UpdateItemCommand(query));
    return {
      ...result,
      toJs: (predicate) => {
        const item = (result.Attributes ? unmarshall(result.Attributes) : {}) as R;
        return predicate ? predicate(item) : item;
      },
    };
  } catch (e) {
    consoleError(e);
    consoleError({ query });
    throw e;
  }
}

/**
 * Update attributes directly without path support, use `updateTableRow` for deep updates
 * @param TableName
 * @param keys
 * @param row
 * @param condExp
 */
type LogicOp = 'AND' | 'OR' | 'NOT';
export async function shallowUpdateTableRow<R>(
  TableName: UpdateItemCommandInput['TableName'],
  keys: Partial<R>,
  row: Partial<R>,
  condExp?:
    | string
    | {
        [x: string]:
          | { op: KeyCondMap['op'] | '<>'; value: string | number; logicOp?: LogicOp }
          | { op: 'IN' | 'BETWEEN'; value: string[]; logicOp?: LogicOp };
      },
) {
  const updateExpressions = [];
  const expressionAttributeValues: {
    [x: string]: any;
  } = {};
  const ExpressionAttributeNames: { [x: string]: string } = {};

  const conditionExpression: string[] = [];
  for (const key in row) {
    if (row.hasOwnProperty(key)) {
      const val = row[key];
      updateExpressions.push(`#${key} = :${key}`);
      expressionAttributeValues[`:${key}`] = val;
      ExpressionAttributeNames[`#${key}`] = key;
    }
  }

  if (typeof condExp === 'object') {
    for (const key in condExp) {
      if (condExp.hasOwnProperty(key)) {
        const exp = condExp[key];
        let cond;
        if (exp.op === 'IN') {
          const valStr = exp.value
            .map((v, i) => {
              const k = `:${key}IN-${i}`;
              expressionAttributeValues[k] = v;
              return k;
            })
            .join(', ');
          cond = `(${key} IN (${valStr}))`;
        }
        if (exp.op === 'BETWEEN') {
          cond = `(${key} between :${key}Xaa and :${key}Xbb)`;
          expressionAttributeValues[`:${key}Xaa`] = exp.value[0];
          expressionAttributeValues[`:${key}Xbb`] = exp.value[1];
        } else {
          cond = `${key} ${exp.op} :${key}Xvv`;
          expressionAttributeValues[`:${key}Xvv`] = exp.value;
        }
        if (exp.logicOp) cond += ` ${exp.logicOp}`;
        conditionExpression.push(cond);
      }
    }
  } else if (typeof condExp === 'string') {
    conditionExpression.push(condExp);
  }

  return updateTableRow(TableName, keys, {
    UpdateExpression: `SET ${updateExpressions.join(', ')}`,
    expressionAttributeValues,
    ExpressionAttributeNames,
    ConditionExpression: conditionExpression.join(' ') || undefined,
  });
}
