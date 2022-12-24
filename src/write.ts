import {
  DeleteItemCommand,
  DeleteItemCommandOutput,
  DeleteItemInput,
  PutItemCommand,
  PutItemCommandOutput,
  PutItemInput,
} from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { getDdbClient } from "./client";
import { DEFAULT_MARSHALL_OPTIONS } from "./constants";
import { consoleError } from "./utils";

/**
 * @param TableName
 * @param data
 * @param params
 */
export async function putTableRow<R>(
  TableName: PutItemInput["TableName"],
  data: Partial<R>,
  params?: Omit<PutItemInput, "TableName" | "Item">,
): Promise<PutItemCommandOutput | null> {
  const client = getDdbClient();
  try {
    const result = await client.send(
      new PutItemCommand({
        TableName,
        Item: marshall(data, { ...DEFAULT_MARSHALL_OPTIONS }),
        ...params,
      }),
    );
    return result || null;
  } catch (e) {
    consoleError(e);
    consoleError({ TableName, data, params });
    throw e;
  }
}

/**
 * @param TableName
 * @param Key
 * @param params
 */
export async function delTableRow<R>(
  TableName: DeleteItemInput["TableName"],
  Key: Partial<R>,
  params?: Omit<DeleteItemInput, "TableName" | "Key">,
): Promise<DeleteItemCommandOutput | null> {
  const client = getDdbClient();
  try {
    const result = await client.send(
      new DeleteItemCommand({
        TableName,
        Key: marshall(Key, { ...DEFAULT_MARSHALL_OPTIONS }),
        ...params,
      }),
    );
    return result || null;
  } catch (e) {
    consoleError(e);
    consoleError({ TableName, Key, params });
    throw e;
  }
}
