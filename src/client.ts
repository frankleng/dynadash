import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";

export let ddbClientInstance: DynamoDBClient | null = null;

export function getDdbClient(): DynamoDBClient {
  if (!ddbClientInstance) {
    ddbClientInstance = new DynamoDBClient({
      requestHandler: new NodeHttpHandler({
        socketTimeout: 10000, // <- this decreases the emfiles count, the Node.js default is 120000
      }),
    });
  }
  return ddbClientInstance;
}

export function setClient(client: DynamoDBClient): void {
  ddbClientInstance = client;
}
