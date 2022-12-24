export type KeyCondMap = { op: "=" | "<>" | ">" | "<" | ">=" | "<="; value: string | number };
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
            op: KeyCondMap["op"] & { op: "<>" };
            value: KeyCondMap["value"];
          }
      );
};
