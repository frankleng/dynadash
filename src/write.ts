import type {
  DeleteItemCommandOutput,
  DeleteItemInput,
  PutItemCommandOutput,
  PutItemInput,
} from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { initDdbClient } from "./client";
import { DEFAULT_MARSHALL_OPTIONS } from "./constants";
import { ConditionExpressionMap } from "./types";
import { consoleError, getConditionExpression } from "./utils";

export function getPutItemInput<R>(
  TableName: PutItemInput["TableName"],
  data: Partial<R>,
  params?: Omit<PutItemInput, "TableName" | "Item"> & { conditionExpressionMapList?: ConditionExpressionMap },
) {
  if (params?.conditionExpressionMapList) {
    const { ConditionExpression, ExpressionAttributeNames, expressionAttributeValues } = getConditionExpression(
      data,
      params.conditionExpressionMapList,
    );
    params = {
      ...params,
      ExpressionAttributeNames,
      ConditionExpression,
    };
    if (expressionAttributeValues) {
      params.ExpressionAttributeValues = marshall(expressionAttributeValues, { ...DEFAULT_MARSHALL_OPTIONS });
    }
  }

  const query: PutItemInput = {
    TableName,
    Item: marshall(data, { ...DEFAULT_MARSHALL_OPTIONS }),
    ...params,
  };

  return query;
}

/**
 * @param TableName
 * @param data
 * @param params
 */
export async function putTableRow<R>(
  TableName: PutItemInput["TableName"],
  data: Partial<R>,
  params?: Omit<PutItemInput, "TableName" | "Item"> & { conditionExpressionMapList?: ConditionExpressionMap },
): Promise<PutItemCommandOutput | null> {
  const client = await initDdbClient();

  const query = getPutItemInput(TableName, data, params);
  try {
    // @ts-ignore
    const result = await client.PutItem(query);
    return result || null;
  } catch (e) {
    consoleError(e);
    consoleError({ query });
    throw e;
  }
}

/**
 * @param TableName
 * @param Key
 * @param params
 */
export async function delTableRow<R>(
  TableName: string,
  Key: Partial<R>,
  params?: Omit<DeleteItemInput, "TableName" | "Key">,
): Promise<DeleteItemCommandOutput | null> {
  const client = await initDdbClient();
  try {
    const result = await client.DeleteItem({
      TableName,
      Key: marshall(Key, { ...DEFAULT_MARSHALL_OPTIONS }),
      ...params,
    });
    return result || null;
  } catch (e) {
    consoleError(e);
    consoleError({ TableName, Key, params });
    throw e;
  }
}
