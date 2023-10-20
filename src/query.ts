import type { QueryCommandInput, QueryCommandOutput } from "@aws-sdk/client-dynamodb";
import { QueryCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { initDdbClient } from "./client";
import { FilterExpressionMap, KeyCondExpressionMap } from "./types";
import { consoleError, getQueryExpression, mergeCapacityStats } from "./utils";

export async function handleQueryCommand<R>(
  query: QueryCommandInput,
  batchCallback?: (rows: R[]) => Promise<any>,
): Promise<
  {
    toJs: { (): R[]; <P>(transform: (row: R) => R | P): (R | P)[] };
  } & QueryCommandOutput
> {
  const client = initDdbClient();
  try {
    let result: QueryCommandOutput | undefined = undefined;
    let items: QueryCommandOutput["Items"] = [];
    const stats: Partial<QueryCommandOutput> = {};

    do {
      // If result exists, set ExclusiveStartKey for pagination
      if (result?.LastEvaluatedKey) {
        query.ExclusiveStartKey = result.LastEvaluatedKey;
      }

      // Sending query to the client and waiting for the result
      result = await client.send(new QueryCommand(query));

      if (batchCallback) {
        await batchCallback([...(result.Items || [])] as R[]);
        result.Items = undefined;
      } else {
        // Concatenating the retrieved items
        items = items.concat(result.Items || []);
      }

      mergeCapacityStats(stats, result);

      // Breaking the loop if items length reaches the query limit
      if (query?.Limit && items.length >= query.Limit) {
        break;
      }
    } while (result.LastEvaluatedKey); // Continue while there is a LastEvaluatedKey

    // Updating the result items with the concatenated items array
    if (result) {
      result.Items = items;
    }

    function toJs(): R[];
    function toJs<P>(transform: (row: R) => R | P): (R | P)[];

    function toJs<P>(transform?: (row: R) => R | P) {
      return result?.Items && result.Items.length > 0
        ? result?.Items.map((row) => {
            const result = unmarshall(row) as R;
            return transform ? transform(result) : result;
          })
        : [];
    }

    return {
      ...result,
      ...stats,
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
 * @param batchCallback
 */
export async function queryTableIndex<R>(
  TableName: QueryCommandInput["TableName"],
  IndexName: QueryCommandInput["IndexName"],
  params?: Partial<Omit<QueryCommandInput, "TableName" | "IndexName">> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
  batchCallback?: (rows: R[]) => Promise<any>,
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

  return handleQueryCommand<R>(query, batchCallback);
}

/**
 * @param TableName
 * @param params
 * @param batchCallback
 */
export async function queryTable<R>(
  TableName: QueryCommandInput["TableName"],
  params?: Partial<Omit<QueryCommandInput, "TableName" | "IndexName">> & {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  },
  batchCallback?: (rows: R[]) => Promise<any>,
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

  return handleQueryCommand<R>(query, batchCallback);
}
