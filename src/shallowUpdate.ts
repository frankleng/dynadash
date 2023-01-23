import { UpdateItemCommandInput } from "@aws-sdk/client-dynamodb";
import { ConditionExpressionMap } from "./types";
import { updateTableRow } from "./update";
import { getConditionExpression } from "./utils";

export async function shallowUpdateTableRow<R>(
  TableName: UpdateItemCommandInput["TableName"],
  keys: Partial<R>,
  row: Partial<R>,
  condExps?: ConditionExpressionMap,
) {
  const { UpdateExpression, ExpressionAttributeNames, expressionAttributeValues, ConditionExpression } =
    getConditionExpression(row, condExps, true);

  return updateTableRow(TableName, keys, {
    UpdateExpression,
    expressionAttributeValues,
    ExpressionAttributeNames,
    ConditionExpression,
  });
}
