import { QueryCommand, QueryCommandInput } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { getDdbClient } from "./client";
import { FilterExpressionMap, KeyCondExpressionMap } from "./types";
import { consoleError, getQueryExpression } from "./utils";

export async function handleQueryCommand<R>(query: QueryCommandInput) {
  const client = getDdbClient();
  try {
    let result = await client.send(new QueryCommand(query));

    if (result?.LastEvaluatedKey && !query.ExclusiveStartKey && !query.Limit) {
      let items = result.Items || [];
      while (result.LastEvaluatedKey) {
        result = await client.send(new QueryCommand({ ...query, ExclusiveStartKey: result.LastEvaluatedKey }));
        items = items.concat(result.Items || []);
      }
      result.Items = items;
    }

    function toJs(): R[];
    function toJs<P>(transform: (row: R) => R | P): (R | P)[];

    function toJs<P>(transform?: (row: R) => R | P) {
      return result.Items?.length
        ? result.Items.map((row) => {
            const result = unmarshall(row) as R;
            return transform ? transform(result) : result;
          })
        : [];
    }

    return {
      ...result,
      toJs,
    };
  } catch (e) {
    consoleError(e);
    consoleError({ query });
    throw e;
  }
}

/**
 * @param TableName
 * @param IndexName
 * @param params
 */
export async function queryTableIndex<R>(
  TableName: QueryCommandInput["TableName"],
  IndexName: QueryCommandInput["IndexName"],
  params?: Partial<Omit<QueryCommandInput, "TableName" | "IndexName">> & {
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
  TableName: QueryCommandInput["TableName"],
  params?: Partial<Omit<QueryCommandInput, "TableName" | "IndexName">> & {
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
