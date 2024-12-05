import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import { Duration } from "aws-cdk-lib";
import { StreamViewType } from "aws-cdk-lib/aws-dynamodb";
import { DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { StartingPosition } from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    const imageTable = new dynamodb.Table(this, "ImageTable", {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "imageId", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "Images",
      stream: StreamViewType.NEW_AND_OLD_IMAGES,
    });

    const badImageQueue = new sqs.Queue(this, "DLQ", {
      retentionPeriod: Duration.minutes(10),
    });

    // Added Dead Letter Queue to Main SQS 
    const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: badImageQueue,
        maxReceiveCount: 1,
      }
    });

    // Lambda functions
    const appCommonFnProps = {
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: "handler",
      environment: {
        TABLE_NAME: imageTable.tableName,
        REGION: cdk.Aws.REGION,
      },
    };

    const updateTable = new lambdanode.NodejsFunction(this, "updateTable", {
        ...appCommonFnProps,
        entry: `${__dirname}/../lambdas/updateTable.ts`,
      }
    );

    const processImageFn = new lambdanode.NodejsFunction(this,"ProcessImageFn", {
        ...appCommonFnProps,
        entry: `${__dirname}/../lambdas/processImage.ts`,
      }
    );

    const rejectionMailerFn = new lambdanode.NodejsFunction(this, "rejectionMailer-function", {
        ...appCommonFnProps,
        entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
      }
    );

    const confirmationMailerFn = new lambdanode.NodejsFunction(this,"mailer-function", {
        ...appCommonFnProps,
        entry: `${__dirname}/../lambdas/confirmationMailer.ts`,
      }
    );

    // SNS topic
    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
    });
    
    newImageTopic.addSubscription(new subs.SqsSubscription(imageProcessQueue, {
      filterPolicyWithMessageBody: {
        Records: sns.FilterOrPolicy.policy({
          eventName: sns.FilterOrPolicy.filter(
            sns.SubscriptionFilter.stringFilter({
              allowlist: ["ObjectCreated:Put", "ObjectRemoved:Delete"]
          })),
        }),
      },
    }));

    newImageTopic.addSubscription(
      new subs.LambdaSubscription(updateTable,
        {
          filterPolicy: {
            metadata_type: sns.SubscriptionFilter.stringFilter({
              allowlist: ['Caption', 'Date', 'Photographer']
            })
          }
        }
      )
    );

    // S3 --> SNS
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)
    );

    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_REMOVED_DELETE,
      new s3n.SnsDestination(newImageTopic) 
    )

    // SQS --> Lambda
    const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });

    // DLQ -- > Lambda
    const newImageRejectionMail = new events.SqsEventSource(badImageQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    })

    rejectionMailerFn.addEventSource(newImageRejectionMail);
    processImageFn.addEventSource(newImageEventSource);

    confirmationMailerFn.addEventSource(  
      new DynamoEventSource(imageTable, {
        startingPosition: StartingPosition.LATEST,
        batchSize: 5,
        retryAttempts: 1,
      })
    )

    // Permissions
    imagesBucket.grantRead(processImageFn);
    imageTable.grantReadWriteData(processImageFn)
    imageTable.grantReadWriteData(updateTable)
    imageTable.grantStreamRead(confirmationMailerFn); 

    confirmationMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    rejectionMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    // Output
    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });

    new cdk.CfnOutput(this, "topicARN", {
      value: newImageTopic.topicArn,
    });
  }
}
