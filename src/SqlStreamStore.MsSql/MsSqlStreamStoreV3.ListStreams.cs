namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class MsSqlStreamStoreV3
    {
        protected override async Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            if(!int.TryParse(continuationToken, out var afterIdInternal))
            {
                afterIdInternal = -1;
            }

            var connection = _createConnection();
            try
            {
                var streamIds = new List<string>();

                await connection.OpenIfRequiredAsync(cancellationToken);
                var transaction = WithTransaction(connection);
                using(var command = GetListStreamsCommand(pattern, maxCount, afterIdInternal, transaction))
                {
                    command.CommandTimeout = _commandTimeout;
                    using(var reader = await command.ExecuteReaderAsync(cancellationToken))
                    {
                        while(await reader.ReadAsync(cancellationToken))
                        {
                            streamIds.Add(reader.GetString(0));
                            afterIdInternal = reader.GetInt32(1);
                        }
                    }
                }

                return new ListStreamsPage(afterIdInternal.ToString(), streamIds.ToArray(), listNextStreamsPage);
            }
            finally
            {
                if (_manageConnection)
                {
                    connection.Dispose();
                }
            }
        }


        private SqlCommand GetListStreamsCommand(
            Pattern pattern,
            int maxCount,
            int? afterIdInternal,
            SqlTransaction transaction)
        {
            switch(pattern)
            {
                case Pattern.Any _:
                    return new SqlCommand(_scripts.ListStreamIds, transaction.Connection, transaction)
                    {
                        Parameters =
                        {
                            new SqlParameter("MaxCount", maxCount),
                            new SqlParameter("AfterIdInternal", afterIdInternal)
                        }
                    };

                case Pattern.StartingWith p:
                    return new SqlCommand(_scripts.ListStreamIdsStartingWith, transaction.Connection, transaction)
                    {
                        Parameters =
                        {
                            new SqlParameter("MaxCount", maxCount),
                            new SqlParameter("AfterIdInternal", afterIdInternal),
                            new SqlParameter("Pattern", p.Value)
                        }
                    };
                case Pattern.EndingWith p:
                    return new SqlCommand(_scripts.ListStreamIdsEndingWith, transaction.Connection, transaction)
                    {
                        Parameters =
                        {
                            new SqlParameter("MaxCount", maxCount),
                            new SqlParameter("AfterIdInternal", afterIdInternal),
                            new SqlParameter("Pattern", p.Value)
                        }
                    };

                default:
                    throw Pattern.Unrecognized(nameof(pattern));
            }
        }
    }
}