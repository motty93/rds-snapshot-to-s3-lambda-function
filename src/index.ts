import { APIGatewayEvent, Context, Handler } from 'aws-lambda'

import {
  CreateDBClusterSnapshotCommand,
  CreateDBClusterSnapshotCommandInput,
  ExportDBClusterSnapshotToS3Command,
  ExportDBClusterSnapshotToS3CommandInput,
  RDSClient,
  waitUntilDBClusterSnapshotAvailable,
} from '@aws-sdk/client-rds'

/**
 * Aurora DB Cluster からスナップショットを作成し、
 * そのスナップショットを S3 にエクスポートする Lambda ハンドラ
 *
 * 環境変数の想定:
 *  DB_CLUSTER_IDENTIFIER (Aurora クラスター名)
 *  SNAPSHOT_NAME        (作成するスナップショット名)
 *  S3_BUCKET_NAME       (エクスポート先の S3 バケット)
 *  IAM_ROLE_ARN         (エクスポート用の IAM ロール)
 *  KMS_KEY_ID           (オプショナル: 暗号化に使う KMS キー ARN)
 *
 * 実行例:
 *  1. Aurora のスナップショットを作成 (CreateDBClusterSnapshot)
 *  2. スナップショットが Available になるまで待機
 *  3. S3 へのエクスポート (ExportDBClusterSnapshotToS3)
 */
export const handler: Handler = async (event: APIGatewayEvent, context: Context) => {
  console.log('Received event:', JSON.stringify(event, null, 2))

  // ---- RDSClient の初期化 ----
  const rdsClient = new RDSClient({})

  // ---- 環境変数からパラメータを取得 ----
  const dbClusterIdentifier = process.env.DB_CLUSTER_IDENTIFIER!
  const snapshotName = process.env.SNAPSHOT_NAME!
  const s3BucketName = process.env.S3_BUCKET_NAME!
  const exportIamRoleArn = process.env.IAM_ROLE_ARN!
  const kmsKeyId = process.env.KMS_KEY_ID // 任意

  try {
    // 1. DB スナップショットを作成
    const createSnapshotParams: CreateDBClusterSnapshotCommandInput = {
      DBClusterIdentifier: dbClusterIdentifier,
      DBClusterSnapshotIdentifier: snapshotName,
    }

    console.log('Creating DB Cluster Snapshot...')
    await rdsClient.send(new CreateDBClusterSnapshotCommand(createSnapshotParams))
    console.log(`Snapshot creation initiated: ${snapshotName}`)

    // 2. スナップショットが "available" になるまで待機
    console.log('Waiting for DB Cluster Snapshot to become available...')
    await waitUntilDBClusterSnapshotAvailable(
      { client: rdsClient, maxWaitTime: 600 }, // 最大 600秒(10分) など調整
      { DBClusterIdentifier: dbClusterIdentifier, DBClusterSnapshotIdentifier: snapshotName },
    )
    console.log(`Snapshot is now available: ${snapshotName}`)

    // 3. スナップショットを S3 にエクスポート
    const exportParams: ExportDBClusterSnapshotToS3CommandInput = {
      ExportTaskIdentifier: `${snapshotName}-export-${Date.now()}`, // 一意にするためにタイムスタンプを付与
      SourceDBClusterSnapshotIdentifier: snapshotName,
      S3BucketName: s3BucketName,
      IamRoleArn: exportIamRoleArn,
      // エクスポート先の S3 プレフィックスを指定したい場合は以下を使う:
      // S3Prefix: 'snapshots/2024-12-26/',
      KmsKeyId: kmsKeyId, // KMSキーによる暗号化をしたい場合
    }

    console.log(`Exporting snapshot to S3 bucket: ${s3BucketName}`)
    const exportResult = await rdsClient.send(new ExportDBClusterSnapshotToS3Command(exportParams))

    console.log('Export initiated:', exportResult)
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Aurora snapshot export to S3 initiated successfully.',
        exportTaskIdentifier: exportResult.ExportTaskIdentifier,
      }),
    }
  } catch (error) {
    console.error('Error occurred:', error)
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: 'Failed to create or export DB cluster snapshot.',
        error: (error as Error).message,
      }),
    }
  }
}
