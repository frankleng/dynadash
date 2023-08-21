import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import https from "https";
const agent = new https.Agent({
  maxSockets: 25,
});
export let ddbClientInstance: DynamoDBClient | null = null;

export function getDdbClient(): DynamoDBClient {
  if (!ddbClientInstance) {
    ddbClientInstance = new DynamoDBClient({
      maxAttempts: 5,
      requestHandler: new NodeHttpHandler({
        httpsAgent: agent,
      }),
    });
  }
  return ddbClientInstance;
}

export function setClient(client: DynamoDBClient): void {
  ddbClientInstance = client;
}
