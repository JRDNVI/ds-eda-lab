import { SQSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SQSHandler = async (event) => {
  console.log("Event ", JSON.stringify(event));

  for (const record of event.Records) {
    const recordBody = JSON.parse(record.body);        // Parse SQS message
    const snsMessage = JSON.parse(recordBody.Message); // Parse SNS message

    if (snsMessage.Records) {
      console.log("Record body ", JSON.stringify(snsMessage));
      for (const messageRecord of snsMessage.Records) {
        const eventName = messageRecord.eventName;
        const s3e = messageRecord.s3;
        const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));

        if (!srcKey.endsWith(".jpeg") && !srcKey.endsWith(".png")) {
          throw new Error("Bad Image");
        }

        try {
          if (eventName === "ObjectCreated:Put") {
            await ddbDocClient.send(
              new PutCommand({
                TableName: process.env.TABLE_NAME,
                Item: {
                  imageId: srcKey
                }
              })
            );

          } else if (eventName === "ObjectRemoved:Delete") {
            await ddbDocClient.send(
              new DeleteCommand({
                TableName: process.env.TABLE_NAME,
                Key: {
                  imageId: srcKey,
                },
              })
            );
          }

        } catch (error) {
          console.log("An Error Occured:", error);
        }
      }
    }
  }
};

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