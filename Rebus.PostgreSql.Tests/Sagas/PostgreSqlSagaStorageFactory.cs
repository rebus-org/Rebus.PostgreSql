using Rebus.Logging;
using Rebus.PostgreSql.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.PostgreSql.Tests.Sagas;

public class PostgreSqlSagaStorageFactory : ISagaStorageFactory
{
    public PostgreSqlSagaStorageFactory()
    {
        PostgreSqlTestHelper.DropTable("sagas", "saga_index");
        PostgreSqlTestHelper.DropTable("sagas","saga_data");
    }

    public ISagaStorage GetSagaStorage()
    {
        var serializer = new DefaultSagaSerializer();
        
        var postgreSqlSagaStorage = new PostgreSqlSagaStorage(PostgreSqlTestHelper.ConnectionHelper, "saga_data", "saga_index", new ConsoleLoggerFactory(false), serializer, schemaName: "sagas");
        postgreSqlSagaStorage.EnsureTablesAreCreated();
        return postgreSqlSagaStorage;
    }

    public void CleanUp()
    {
        //PostgreSqlTestHelper.DropTable("saga_index");
        //PostgreSqlTestHelper.DropTable("saga_data");
    }
}