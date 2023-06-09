import { UpdateItemCommand, UpdateItemCommandInput, UpdateItemCommandOutput } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { getDdbClient } from "./client";
import { DEFAULT_MARSHALL_OPTIONS, DB_RETURN_VALUE } from "./constants";
import { DbReturnValue } from "./types";
import { consoleError } from "./utils";

export async function updateTableRow<R>(
  TableName: UpdateItemCommandInput["TableName"],
  keys: Partial<R>,
  params: {
    UpdateExpression: string;
    expressionAttributeValues: { [x: string]: any } | undefined;
    ExpressionAttributeNames?: { [x: string]: string };
    ConditionExpression?: string;
  },
  ReturnValues: DbReturnValue = DB_RETURN_VALUE.NONE,
): Promise<UpdateItemCommandOutput & { toJs: (iterator?: (row: R) => R) => R }> {
  const { UpdateExpression, expressionAttributeValues, ExpressionAttributeNames, ConditionExpression } = params;
  const client = getDdbClient();
  try {
    const query: UpdateItemCommandInput = {
      TableName,
      Key: marshall(keys, { ...DEFAULT_MARSHALL_OPTIONS }),
      UpdateExpression,
      ExpressionAttributeValues: marshall(expressionAttributeValues, {
        ...DEFAULT_MARSHALL_OPTIONS,
      }),
      ConditionExpression,
      ExpressionAttributeNames,
      ReturnValues,
    };
    const result = await client.send(new UpdateItemCommand(query));
    return {
      ...result,
      toJs: (iterator) => {
        const item = (result.Attributes ? unmarshall(result.Attributes) : {}) as R;
        return iterator ? iterator(item) : item;
      },
    };
  } catch (e) {
    consoleError(e);
    consoleError({ TableName, keys, params, ReturnValues });
    throw e;
  }
}
