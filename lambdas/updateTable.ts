import { SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event) => {
  console.log("SNS Event received:", JSON.stringify(event));

  for (const record of event.Records) {
    try {

      const message = JSON.parse(record.Sns.Message);
      const id = message.id;
      const value = message.value;
      const metadataType = record.Sns.MessageAttributes.metadata_type.Value;

      await ddbDocClient.send(
        new UpdateCommand({
          TableName: process.env.TABLE_NAME,
          Key: { imageId: id },
          UpdateExpression: `SET ${metadataType} = :value`,
          ExpressionAttributeValues: {
            ":value": value
          },
        })
      )

      console.log("Item Updated:", id)
    } catch (error) {
      console.error("An Error occurred:", error)
    }
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