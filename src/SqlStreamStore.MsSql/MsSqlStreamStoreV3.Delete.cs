namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using static Streams.Deleted;

    public partial class MsSqlStreamStoreV3
    {
        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);
            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        protected override async Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            var connection = _createConnection();
            try
            {
                await connection.OpenIfRequiredAsync(cancellationToken);

                var transaction = WithTransaction(connection);
                var sqlStreamId = new StreamIdInfo(streamId).SqlStreamId;

                bool deleted;
                using (var command = new SqlCommand(_scripts.DeleteStreamMessage, connection, transaction))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = sqlStreamId.Id });
                    command.Parameters.AddWithValue("eventId", eventId);
                    var count = await command
                        .ExecuteScalarAsync(cancellationToken)
                        ;

                    deleted = (int)count == 1;
                }

                if (deleted && !_settings.DisableDeletionTracking)
                {
                    var eventDeletedEvent = CreateMessageDeletedMessage(sqlStreamId.IdOriginal, eventId);
                    await AppendToStreamExpectedVersionAny(
                        connection,
                        transaction,
                        SqlStreamId.Deleted,
                        new[] { eventDeletedEvent },
                        cancellationToken);
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
        }

        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var connection = _createConnection();
            try
            {
                await connection.OpenIfRequiredAsync(cancellationToken);

                var transaction = WithTransaction(connection);
                using (var command = new SqlCommand(_scripts.DeleteStreamExpectedVersion, connection, transaction))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = streamIdInfo.SqlStreamId.Id });
                    command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                    try
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            ;
                    }
                    catch (SqlException ex)
                    {
                        if (_manageConnection)
                        {
                            transaction.Rollback();
                            transaction.Dispose();
                        }
                        if (ex.Message.StartsWith("WrongExpectedVersion"))
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamIdInfo.SqlStreamId.IdOriginal, expectedVersion),
                                streamIdInfo.SqlStreamId.IdOriginal,
                                expectedVersion,
                                ex);
                        }
                        throw;
                    }

                    if (!_settings.DisableDeletionTracking)
                    {
                        var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.SqlStreamId.IdOriginal);
                        await AppendToStreamExpectedVersionAny(
                            connection,
                            transaction,
                            SqlStreamId.Deleted,
                            new[] { streamDeletedEvent },
                            cancellationToken);
                    }

                    // Delete metadata stream (if it exists)
                    await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.MetadataSqlStreamId, cancellationToken);

                    if (_manageConnection)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                    }
                }
            }
            finally
            {
                if (_manageConnection)
                {
                    connection.Dispose();
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            var connection = _createConnection();
            try
            {
                await connection.OpenIfRequiredAsync(cancellationToken);

                var transaction = WithTransaction(connection);

                await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.SqlStreamId, cancellationToken);

                // Delete metadata stream (if it exists)
                await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.MetadataSqlStreamId, cancellationToken);

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
        }

        private async Task DeleteStreamAnyVersion(
           SqlConnection connection,
           SqlTransaction transaction,
           SqlStreamId sqlStreamId,
           CancellationToken cancellationToken)
        {
            bool aStreamIsDeleted;
            using (var command = new SqlCommand(_scripts.DeleteStreamAnyVersion, connection, transaction))
            {
                command.CommandTimeout = _commandTimeout;
                command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = sqlStreamId.Id });
                var i = await command
                    .ExecuteScalarAsync(cancellationToken)
                    ;

                aStreamIsDeleted = (int)i > 0;
            }

            if(aStreamIsDeleted && !_settings.DisableDeletionTracking)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(sqlStreamId.IdOriginal);
                await AppendToStreamExpectedVersionAny(
                    connection,
                    transaction,
                    SqlStreamId.Deleted,
                    new[] { streamDeletedEvent },
                    cancellationToken);
            }
        }
    }
}
