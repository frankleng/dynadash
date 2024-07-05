import { AwsLiteDynamoDB } from "@aws-lite/dynamodb-types";
import type { DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";
import awsLite from "aws-lite-fetch-client";

export let ddbClientInstance: AwsLiteDynamoDB | null = null;

export async function initDdbClient(
  credentialDefaultProvider?: DynamoDBClientConfig["credentialDefaultProvider"],
  maxAttempts = 10,
  retryMode: DynamoDBClientConfig["retryMode"] = "adaptive",
): Promise<AwsLiteDynamoDB> {
  if (!ddbClientInstance) {
    // @ts-ignore
    const aws = await awsLite({
      // @ts-ignore
      plugins: [import("@aws-lite/dynamodb")],
      retry: maxAttempts || 10,
    });
    // @ts-ignore
    ddbClientInstance = aws.DynamoDB as AwsLiteDynamoDB;
  }
  return ddbClientInstance;
}

export function setClient(client: AwsLiteDynamoDB): void {
  ddbClientInstance = client;
}
