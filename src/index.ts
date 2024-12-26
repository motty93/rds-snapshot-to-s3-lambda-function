import { RDSClient, StartExportTaskCommand, StartExportTaskCommandInput } from '@aws-sdk/client-rds'
import { APIGatewayEvent, Context, Handler } from 'aws-lambda'

const SourceArn = process.env.SOURCE_ARN || ''
const S3BucketName = process.env.S3_BUCKET_NAME || ''
const IamRoleArn = process.env.IAM_ROLE_ARN || ''
const KmsKeyId = process.env.KMS_KEY_ID || ''

export const handler: Handler = async (event: APIGatewayEvent, _context: Context) => {
  console.log('Received event:', JSON.stringify(event, null, 2))

  const exportTaskIdentifier = `snapshot${new Date().toISOString().replace(/[-T:.Z]/g, '')}`
  const client = new RDSClient({})
  const params: StartExportTaskCommandInput = {
    ExportTaskIdentifier: exportTaskIdentifier,
    SourceArn,
    S3BucketName,
    IamRoleArn,
    KmsKeyId,
    // S3Prefix: 'my-optional-prefix/', // 任意でプレフィックスを付けられる
  }

  try {
    const response = await client.send(new StartExportTaskCommand(params))
    console.log('Export Task started:', response)

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Export task started successfully',
        exportTaskIdentifier: exportTaskIdentifier,
      }),
    }
  } catch (error) {
    console.error('Error while starting export task:', error)
    return {
      statusCode: 500,
      body: JSON.stringify({ message: 'Failed to start export task', error }),
    }
  }
}
