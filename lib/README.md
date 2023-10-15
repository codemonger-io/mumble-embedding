## Development

### Setting OPENAI_API_KEY

```sh
read -s OPENAI_API_KEY
```

```sh
export OPENAI_API_KEY
```

### Setting AWS_PROFILE

```sh
export AWS_PROFILE=codemonger-jp
```

### Setting DEPLOYMENT_STAGE

```sh
DEPLOYMENT_STAGE=development
```

```sh
DEPLOYMENT_STAGE=production
```

### Obtaining the objects bucket name

```sh
export OBJECTS_BUCKET_NAME=`aws cloudformation describe-stacks --stack-name mumble-$DEPLOYMENT_STAGE --query "Stacks[0].Outputs[?OutputKey=='ObjectsBucketName'].OutputValue" --output text`
```

### Obtaining the database bucket name

```sh
export DATABASE_BUCKET_NAME=`aws cloudformation describe-stacks --stack-name mumble-$DEPLOYMENT_STAGE --query "Stacks[0].Outputs[?OutputKey=='IndexerDatabaseBucketName'].OutputValue" --output text`
```