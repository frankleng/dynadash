import type { GetItemCommandOutput, GetItemInput } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { initDdbClient } from "./client";
import { DEFAULT_MARSHALL_OPTIONS } from "./constants";
import { consoleError } from "./utils";

/**
 * @param TableName
 * @param keys
 * @param params
 */
export async function getTableRow<R>(
  TableName: GetItemInput["TableName"],
  keys: { [x: string]: any },
  params?: Omit<GetItemInput, "TableName" | "Key"> & { projection?: string[] },
): Promise<GetItemCommandOutput & { toJs: () => R | null }> {
  const { projection, ...rest } = params || {};
  const client = await initDdbClient();
  try {
    const query: GetItemInput = {
      TableName,
      Key: marshall(keys, { ...DEFAULT_MARSHALL_OPTIONS }),
      ...rest,
    };
    if (projection) query["ProjectionExpression"] = projection.join(",");

    // @ts-ignore
    const result = await client.GetItem(query);
    return { ...result, toJs: () => (result.Item ? (unmarshall(result.Item) as R) : null) };
  } catch (e) {
    consoleError(e);
    consoleError({ TableName, keys, params });
    throw e;
  }
}
