namespace SqlStreamStore
{
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using StreamStoreStore.Json;

    public partial class MsSqlStreamStoreV3
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            ReadStreamPage page;
            var connection = _createConnection();
            try
            {
                await connection.OpenIfRequiredAsync(cancellationToken).NotOnCapturedContext();
                (page, _) = await ReadStreamInternal(
                    streamIdInfo.MetadataSqlStreamId,
                    StreamVersion.End,
                    1,
                    ReadDirection.Backward,
                    true,
                    null,
                    connection,
                    null,
                    cancellationToken);
            }
            finally
            {
                if (_manageConnection)
                {
                    connection.Dispose();
                }
            }

            if(page.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamId, -1);
            }

            var metadataMessage = await page.Messages[0].GetJsonDataAs<MetadataMessage>(cancellationToken);

            return new StreamMetadataResult(
                   streamId,
                   page.LastStreamVersion,
                   metadataMessage.MaxAge,
                   metadataMessage.MaxCount,
                   metadataMessage.MetaJson);
        }

        protected override async Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            MsSqlAppendResult result;
            var connection = _createConnection();
            try
            {
                var streamIdInfo = new StreamIdInfo(streamId);

                await connection.OpenIfRequiredAsync(cancellationToken).NotOnCapturedContext();

                var transaction = WithTransaction(connection);

                var metadataMessage = new MetadataMessage
                {
                    StreamId = streamId,
                    MaxAge = maxAge,
                    MaxCount = maxCount,
                    MetaJson = metadataJson
                };
                var json = SimpleJson.SerializeObject(metadataMessage);
                var messageId = MetadataMessageIdGenerator.Create(
                    streamId,
                    expectedStreamMetadataVersion,
                    json);
                var newStreamMessage = new NewStreamMessage(messageId, "$stream-metadata", json);

                result = await AppendToStreamInternal(
                    connection,
                    transaction,
                    streamIdInfo.MetadataSqlStreamId,
                    expectedStreamMetadataVersion,
                    new[] { newStreamMessage },
                    cancellationToken);

                using (var command = new SqlCommand(_scripts.SetStreamMetadata, connection, transaction))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = streamIdInfo.SqlStreamId.Id });
                    command.Parameters.AddWithValue("streamIdOriginal", streamIdInfo.SqlStreamId.IdOriginal);
                    command.Parameters.Add("maxAge", SqlDbType.Int);
                    command.Parameters["maxAge"].Value = maxAge ?? -1;
                    command.Parameters.Add("maxCount", SqlDbType.Int);
                    command.Parameters["maxCount"].Value = maxCount ?? -1;
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }

                if (_manageConnection)
                {
                    transaction.Commit();
                    transaction.Dispose();
                }
            }
            finally
            {
                if (_manageConnection)
                {
                    connection.Dispose();
                }
            }

            await CheckStreamMaxCount(streamId, maxCount, cancellationToken);

            return new SetStreamMetadataResult(result.CurrentVersion);
        }
    }
}
