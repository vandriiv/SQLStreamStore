using Microsoft.Data.SqlClient;

namespace SqlStreamStore
{
    public partial class MsSqlStreamStoreV3
    {
        private readonly bool _manageConnection;

        private SqlTransaction WithTransaction(SqlConnection connection)
        {
            return _settings.ScopeTransaction ?? connection.BeginTransaction();
        }
    }
}
