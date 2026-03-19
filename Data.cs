using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Database.Connection;
using Database.Entity;
using Database.Enums;
using Database.Exceptions;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;

namespace Database;

/// <summary>
/// Used to interact directly with the database utilizing SQL commands.
///
/// You must initialize this before using.
/// </summary>
public static class Data
{
    // ReSharper disable once InconsistentNaming
    private const int COMMAND_TIMEOUT = 180;

    /// <summary>
    /// Initializes Data class
    /// </summary>
    /// <param name="defaultConnectionProperties">Default connection properties used to connect to the database</param>
    public static void Initialize(ConnectionProperties defaultConnectionProperties)
    {
        if (defaultConnectionProperties == null)
        {
            throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please supply the ConnectionProperties to Data.Initialize(ConnectionProperties).");
        }
        
        Environment.Initialize(defaultConnectionProperties);
    }

    public static T GetEntity<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        var table = GetDataTable(command, dbCommandType);
        var row = table.Rows[0];

        return row.ToEntity<T>();
    }
    
    public static IList<T> GetEntityList<T>(string command,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        return GetDataTable(command, dbCommandType).ToEntities<T>().ToList();
    }
    
    public static IList<T> GetEntityList<T>(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        return GetDataTable(command, parameters, dbCommandType).ToEntities<T>().ToList();
    }
    
    public static IList<T> GetEntityList<T>(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        return GetDataTable(command, parameters, connectionName, dbCommandType).ToEntities<T>().ToList();
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataSet GetDataSet(string command, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection.IsValid())
            return GetDataSet(command, null, Environment.Connections.DefaultConnection.ConnectionName);
        else
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataSet GetDataSet(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection.IsValid())
            return GetDataSet(command, parameters, Environment.Connections.DefaultConnection.ConnectionName);
        else
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataSet GetDataSet(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        return FillDatSet(command, parameters, connectionName);
    }

    /// <summary>
    /// Execute the provided command using the default connection and return the results in a DataTable.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (!Environment.Connections.DefaultConnection.IsValid())
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return GetDataTable(command, null, Environment.Connections.DefaultConnection.ConnectionName);
    }

    /// <summary>
    /// Execute the provided command using the default connection and return the results in a DataTable.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (!Environment.Connections.DefaultConnection.IsValid())
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return GetDataTable(command, parameters, Environment.Connections.DefaultConnection.ConnectionName);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        return GetDataTable(command, parameters, connectionName, 0);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="tableIndex">Index within tables collection to return</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, Parameters parameters, string connectionName, int tableIndex,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        return FillDatSet(command, parameters, connectionName, dbCommandType).Tables[tableIndex];
    }

    /// <summary>
    /// Returns a filled dataset
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameters</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns/>
    private static DataSet FillDatSet(string command, IEnumerable<DbParameter> parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        DbServerType dbServerType = Environment.Connections[connectionName].DbServerType;
        string connectionString = Environment.Connections[connectionName].ConnectionString;
        var dataSet = new DataSet();

        switch (dbServerType)
        {
            case DbServerType.postgresql:
                var npgsqlCommand = new NpgsqlCommand(command, new NpgsqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };

                if (parameters != null)
                {
                    foreach (var sqlParameter in parameters)
                    {
                        npgsqlCommand.Parameters.Add(sqlParameter);
                    }
                }

                var npgsqlDataAdapter = new NpgsqlDataAdapter(npgsqlCommand);
                
                npgsqlDataAdapter.Fill(dataSet);

                npgsqlCommand.Connection?.Close();

                npgsqlDataAdapter.Dispose();

                break;
            case DbServerType.mssql:
                var sqlCommand = new SqlCommand(command, new SqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };

                if (parameters != null)
                {
                    foreach (var sqlParameter in parameters)
                    {
                        sqlCommand.Parameters.Add(sqlParameter);
                    }
                }

                var sqlDataAdapter = new SqlDataAdapter(sqlCommand);
                
                sqlDataAdapter.Fill(dataSet);

                sqlCommand.Connection.Close();

                sqlDataAdapter.Dispose();

                break;
            default:
                throw new ArgumentOutOfRangeException();
        }


        return dataSet;
    }

    /// <summary>
    /// Executes the provided command using the default connection and returns an open DataReader.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader GetDataReader(string command, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (!Environment.Connections.DefaultConnection.IsValid())
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return GetDataReader(command, null, Environment.Connections.DefaultConnection.ConnectionName,
                dbCommandType);
    }

    /// <summary>
    /// Executes the provided command using the default connection and returns an open <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader GetDataReader(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (!Environment.Connections.DefaultConnection.IsValid())
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return GetDataReader(command, parameters, Environment.Connections.DefaultConnection.ConnectionName,
                dbCommandType);
    }

    /// <summary>
    /// Executes the provided command using the default connection and returns an open DataReader.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader GetDataReader(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        if (!Environment.Connections[connectionName].IsValid())
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return CreateDataReader(command, parameters, Environment.Connections[connectionName].ConnectionName,
                dbCommandType);
    }

    /// <summary>
    /// Executes the provided command and returns an open DataReader.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader CreateDataReader(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        DbCommand dbCommand;
        DbServerType dbServerType = Environment.Connections[connectionName].DbServerType;
        string connectionString = Environment.Connections[connectionName].ConnectionString;

        switch (dbServerType)
        {
            case DbServerType.postgresql:
                dbCommand = new NpgsqlCommand(command, new NpgsqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = 180
                };
                break;
            case DbServerType.mssql:
                dbCommand = new SqlCommand(command, new SqlConnection(connectionString))
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 180
                };
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(dbServerType), dbServerType, null);
        }


        if (parameters != null)
        {
            foreach (var sqlParameter in parameters)
            {
                dbCommand.Parameters.Add(sqlParameter);
            }
        }

        if (dbCommand.Connection != null
            && dbCommand.Connection.State != ConnectionState.Open)
        {
            dbCommand.Connection.Open();
        }

        return dbCommand.ExecuteReader();
    }

    /// <summary>
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string command,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection == null)
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return Execute(command, null, Environment.Connections.DefaultConnection.ConnectionName);
    }

    /// <summary>
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection == null)
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return Execute(command, parameters, Environment.Connections.DefaultConnection.ConnectionName);
    }

    /// <summary>
    /// Executes the provided command.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Command RETURN value.
    /// </returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public static int Execute(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        DbCommand dbCommand;
        var dbServerType = Environment.Connections[connectionName].DbServerType;
        var connectionString = Environment.Connections[connectionName].ConnectionString;
        DbParameter returnParameter;

        switch (dbServerType)
        {
            case DbServerType.postgresql:
                dbCommand = new NpgsqlCommand(command, new NpgsqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };

                returnParameter = new NpgsqlParameter("p_out", NpgsqlDbType.Integer)
                    { Direction = ParameterDirection.Output };

                break;
            case DbServerType.mssql:
                dbCommand = new SqlCommand(command, new SqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };

                returnParameter = new SqlParameter("@RETURN_VALUE", SqlDbType.Int)
                    { Direction = ParameterDirection.ReturnValue };

                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        dbCommand.Parameters.Add(returnParameter);

        if (parameters != null)
        {
            foreach (var sqlParameter in parameters)
            {
                dbCommand.Parameters.Add(sqlParameter);
            }
        }

        if (dbCommand.Connection != null
            && dbCommand.Connection.State != ConnectionState.Open)
        {
            dbCommand.Connection.Open();
        }

        dbCommand.ExecuteNonQuery();
        
        dbCommand.Connection?.Close();

        return Convert.ToInt32(returnParameter.Value);
    }

    /// <summary>
    /// Executes the provided command and returns a single value.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string command,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection == null)
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return ExecuteScalar(command, null, Environment.Connections.DefaultConnection.ConnectionName);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection == null)
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return ExecuteScalar(command, parameters, Environment.Connections.DefaultConnection.ConnectionName);
    }

    /// <summary>
    /// Executes the provided command and returns a single value.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    /// <returns>
    /// Object containing the result
    /// </returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public static object ExecuteScalar(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        DbCommand dbCommand;
        var dbServerType = Environment.Connections[connectionName].DbServerType;
        var connectionString = Environment.Connections[connectionName].ConnectionString;

        switch (dbServerType)
        {
            case DbServerType.postgresql:
                dbCommand = new NpgsqlCommand(command, new NpgsqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };
                break;
            case DbServerType.mssql:
                dbCommand = new SqlCommand(command, new SqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        if (parameters != null)
        {
            foreach (var sqlParameter in parameters)
            {
                dbCommand.Parameters.Add(sqlParameter);
            }
        }

        if (dbCommand.Connection != null
            && dbCommand.Connection.State != ConnectionState.Open)
        {
            dbCommand.Connection.Open();
        }

        var obj = dbCommand.ExecuteScalar();

        dbCommand.Connection?.Close();

        return obj;
    }

    /// <summary>
    /// Executes the provided command and returns a single value using the default connection.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    /// <exception cref="DefaultConnectionNotDefined">The Default Connection has not been defined. Please define prior to use.</exception>
    public static T ExecuteScalar<T>(string command,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection == null)
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return ExecuteScalar<T>(command, null, Environment.Connections.DefaultConnection.ConnectionName,
                dbCommandType);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value using the default connection.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Result as T
    /// </returns>
    /// <exception cref="DefaultConnectionNotDefined">The Default Connection has not been defined. Please define prior to use.</exception>
    public static T ExecuteScalar<T>(string command, Parameters parameters,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        if (Environment.Connections.DefaultConnection == null)
            throw new DefaultConnectionNotDefined(
                "The Default Connection has not been defined. Please define prior to use.");
        else
            return ExecuteScalar<T>(command, parameters, Environment.Connections.DefaultConnection.ConnectionName,
                dbCommandType);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Result as T
    /// </returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public static T ExecuteScalar<T>(string command, Parameters parameters, string connectionName,
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        if (!Environment.Initialized)
        {
            throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
        }
        
        ValidateConnection(connectionName);

        DbCommand dbCommand;
        var dbServerType = Environment.Connections[connectionName].DbServerType;
        var connectionString = Environment.Connections[connectionName].ConnectionString;

        switch (dbServerType)
        {
            case DbServerType.postgresql:
                dbCommand = new NpgsqlCommand(command, new NpgsqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };
                break;
            case DbServerType.mssql:
                dbCommand = new SqlCommand(command, new SqlConnection(connectionString))
                {
                    CommandType = dbCommandType,
                    CommandTimeout = COMMAND_TIMEOUT
                };

                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        if (parameters != null)
        {
            foreach (var sqlParameter in parameters)
            {
                dbCommand.Parameters.Add(sqlParameter);
            }
        }

        if (dbCommand.Connection != null
            && dbCommand.Connection.State != ConnectionState.Open)
        {
            dbCommand.Connection.Open();
        }

        var obj = dbCommand.ExecuteScalar();

        dbCommand.Connection?.Close();

        return (T)obj;
    }


    /// <summary>
    /// Validates that given connections keys/names are valid
    /// </summary>
    /// <param name="connectionName">Name of connection properties stored internally.</param>
    private static void ValidateConnection(string connectionName)
    {
        if (connectionName == string.Empty)
            throw new UnknownConnection("Connection key/name cannot be empty.");

        if (!Environment.Connections[connectionName].IsValid())
            throw new UnknownConnection("Connection key/name '" + connectionName + "' refers to an unknown connection.");
    }
}