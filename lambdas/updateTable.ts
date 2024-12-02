import { SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  UpdateCommand,
  UpdateCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event: any) => {
  console.log("SNS Event received:", JSON.stringify(event));

  try {
    for (const record of event.Records) {
      const sns = record.Sns;
      console.log("SNS Record:", JSON.stringify(sns));

      // Parse the message string into JSON
      const message = JSON.parse(sns.Message);
      console.log("Parsed Message:", message);

      // Access values from the parsed message
      const id = message.id;
      const value = message.value;
      console.log("ID:", id);
      console.log("Value:", value);

      // Access message attributes
      const metadataType = sns.MessageAttributes.metadata_type?.Value;
      console.log("Metadata Type:", metadataType);

      await ddbDocClient.send(
        new UpdateCommand({
          TableName: process.env.TABLE_NAME,
          Key: { imageId: id },
          UpdateExpression: "SET #message = :message",
          ExpressionAttributeNames: {
            "#message": "message"
          },
          ExpressionAttributeValues: {
            ":message": value
          },
        })
      )
    }
  } catch (e) {
      
  }
}


function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}