import * as path from 'node:path';
import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_lambda as lambda,
  aws_s3 as s3,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

import { RustFunction } from 'cargo-lambda-cdk';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const databaseBucket = new s3.Bucket(this, 'DatabaseBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    const queryLambda = new RustFunction(this, 'QueryLambda', {
      architecture: lambda.Architecture.ARM_64,
      manifestPath: path.join('lambda', 'database', 'Cargo.toml'),
      binaryName: 'query',
      environment: {
        DATABASE_BUCKET_NAME: databaseBucket.bucketName,
      },
      memorySize: 256,
      timeout: Duration.seconds(30),
    });
    databaseBucket.grantRead(queryLambda);

    new CfnOutput(this, 'DatabaseBucketName', {
      description: 'Name of the S3 bucket for database',
      value: databaseBucket.bucketName,
    });
  }
}
