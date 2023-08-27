import { DynamoDBClient, DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import https from "https";
import http from "http";

export let ddbClientInstance: DynamoDBClient | null = null;

export function initDdbClient(
  credentialDefaultProvider?: DynamoDBClientConfig["credentialDefaultProvider"],
  maxAttempts = 5,
): DynamoDBClient {
  if (!ddbClientInstance) {
    ddbClientInstance = new DynamoDBClient({
      credentialDefaultProvider,
      maxAttempts,
      requestHandler: new NodeHttpHandler({
        socketTimeout: 10000,
        connectionTimeout: 10000,
        httpsAgent: new https.Agent({
          maxSockets: 50,
        }),
        httpAgent: new http.Agent({
          maxSockets: 50,
        }),
      }),
    });
  }
  return ddbClientInstance;
}

export function setClient(client: DynamoDBClient): void {
  ddbClientInstance = client;
}
