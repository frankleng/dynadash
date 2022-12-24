import { UpdateItemCommandInput } from "@aws-sdk/client-dynamodb";
import { KeyCondMap } from "./types";
import { updateTableRow } from "./update";

/**
 * Update attributes directly without path support, use `updateTableRow` for deep updates
 * @param TableName
 * @param keys
 * @param row
 * @param condExp
 */
export type LogicOp = "AND" | "OR" | "NOT";

export async function shallowUpdateTableRow<R>(
  TableName: UpdateItemCommandInput["TableName"],
  keys: Partial<R>,
  row: Partial<R>,
  condExps?:
    | string
    | (
        | { key: string; op: KeyCondMap["op"] | "<>"; value: string | number; logicOp?: LogicOp; func?: undefined }
        | { key: string; op: "IN" | "BETWEEN"; value: string[]; logicOp?: LogicOp; func?: undefined }
        | {
            key: string;
            func: "attribute_not_exists" | "attribute_exists" | "size";
            logicOp?: LogicOp;
            op?: KeyCondMap["op"];
            value?: string | number;
          }
      )[],
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

  if (Array.isArray(condExps)) {
    for (const condExp of condExps) {
      const { key, op, value, func, logicOp } = condExp;
      ExpressionAttributeNames[`#${key}`] = key;

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
      } else if (op === "BETWEEN") {
        cond = `(#${key} between :${key}Xaa and :${key}Xbb)`;
        expressionAttributeValues[`:${key}Xaa`] = value[0];
        expressionAttributeValues[`:${key}Xbb`] = value[1];
      } else if (typeof op === "undefined" && func) {
        if (!op) {
          cond = `${func}(#${key})`;
        } else {
          cond = `${func}(#${key}) ${op} :${key}Xvv`;
          expressionAttributeValues[`:${key}Xvv`] = value;
        }
      } else {
        cond = `#${key} ${op} :${key}Xvv`;
        expressionAttributeValues[`:${key}Xvv`] = value;
      }

      if (logicOp) cond += ` ${logicOp}`;
      conditionExpression.push(cond);
    }
  } else if (typeof condExps === "string") {
    conditionExpression.push(condExps);
  }

  return updateTableRow(TableName, keys, {
    UpdateExpression: `SET ${updateExpressions.join(", ")}`,
    expressionAttributeValues,
    ExpressionAttributeNames,
    ConditionExpression: conditionExpression.join(" ") || undefined,
  });
}
