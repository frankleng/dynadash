import { UpdateItemCommandInput } from "@aws-sdk/client-dynamodb";
import type { ConditionExpressionMap, DbReturnValue } from "./types";
import { DB_RETURN_VALUE } from "./constants";
import { updateTableRow } from "./update";
import { getConditionExpression } from "./utils";

export async function shallowUpdateTableRow<R>(
  TableName: UpdateItemCommandInput["TableName"],
  keys: Partial<R>,
  row: Partial<R>,
  condExps?: ConditionExpressionMap,
  ReturnValues: DbReturnValue = DB_RETURN_VALUE.NONE,
) {
  const { UpdateExpression, ExpressionAttributeNames, expressionAttributeValues, ConditionExpression } =
    getConditionExpression(row, condExps, true);

  return updateTableRow(
    TableName,
    keys,
    {
      UpdateExpression,
      expressionAttributeValues,
      ExpressionAttributeNames,
      ConditionExpression,
    },
    ReturnValues,
  );
}
