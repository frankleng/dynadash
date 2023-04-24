export type KeyCondMap = { op: "=" | "<>" | ">" | "<" | ">=" | "<="; value: string | number };

export type KeyCondBetweenMap = { op: "BETWEEN"; low: string; high: string };

export type KeyCondExpressionMap = {
  [key: string]: string | number | KeyCondMap | KeyCondBetweenMap;
};
export type FilterExpressionMap = {
  [key: string]:
    | string
    | number
    | (
        | KeyCondMap
        | {
            op: KeyCondMap["op"] & { op: "<>" };
            value: KeyCondMap["value"];
          }
      );
};

/**
 * Update attributes directly without path support, use `updateTableRow` for deep updates
 * @param TableName
 * @param keys
 * @param row
 * @param condExp
 */
export type LogicOp = "AND" | "OR" | "NOT";

export type ConditionExpressionMap =
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
    )[];
