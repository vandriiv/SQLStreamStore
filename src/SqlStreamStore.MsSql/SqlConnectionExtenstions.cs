using Microsoft.Data.SqlClient;
using SqlStreamStore.Infrastructure;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace SqlStreamStore
{
    public static class SqlConnectionExtenstions
    {
        public static async Task OpenIfRequiredAsync(this SqlConnection dbConnection, CancellationToken cancellationToken)
        {
            if (dbConnection.State!= ConnectionState.Open)
            {
                await dbConnection.OpenAsync(cancellationToken);
            }
        }
    }
}
