import { DynamoDBClient, DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";

export let ddbClientInstance: DynamoDBClient | null = null;

export function initDdbClient(
  credentialDefaultProvider?: DynamoDBClientConfig["credentialDefaultProvider"],
  maxAttempts = 5,
): DynamoDBClient {
  if (!ddbClientInstance) {
    const config: DynamoDBClientConfig = {
      maxAttempts,
    };
    if (credentialDefaultProvider) {
      config.credentialDefaultProvider = credentialDefaultProvider;
    }
    ddbClientInstance = new DynamoDBClient(config);
  }
  return ddbClientInstance;
}

export function setClient(client: DynamoDBClient): void {
  ddbClientInstance = client;
}
