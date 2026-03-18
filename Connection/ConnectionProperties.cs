using Database.Enums;

namespace Database.Connection;

public class ConnectionProperties
{
    public string ConnectionName { get; set; }

    public string ConnectionString { get; set; }

    public DbServerType DbServerType { get; set; }
    
    /// <summary>
    /// The default database schema to use.
    /// Defaults to "dbo"
    /// </summary>
    public string DefaultDbSchema { get; set; } = "dbo";

    public ConnectionProperties(string connectionName, string conectionString, DbServerType dbServerType)
    {
        ConnectionName = connectionName;
        ConnectionString = conectionString;
        DbServerType = dbServerType;
    }

    public bool IsValid()
    {
        return !string.IsNullOrEmpty(ConnectionName) 
               && !string.IsNullOrEmpty(ConnectionString);
    }
}