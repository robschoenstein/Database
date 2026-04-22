using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
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
public class DataAccess
{

    private const int CommandTimeout = 180;

    /// <summary>
    /// Initializes DataAccess class and the underlying Environment storage class
    /// </summary>
    /// <param name="defaultConnectionProperties">Default connection properties used to connect to the database</param>
    public DataAccess(ConnectionProperties defaultConnectionProperties)
    {
        ArgumentNullException.ThrowIfNull(defaultConnectionProperties);
        if (!Environment.IsInitialized)
        {
            Environment.Initialize(defaultConnectionProperties);
        }
    }

    /// <summary>
    /// Retrieves a row from the database and converts to the specified entity type.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="dbCommandType">Type of SQL command to execute. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>An entity</returns>
    /// <exception cref="DataNotInitialized">If the caller attempts to utilize this library without creating an instance of this class.</exception>
    public static T GetEntity<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetEntity<T>(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Retrieves a row from the database and converts to the specified entity type.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">Type of SQL command to execute. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>An entity.</returns>
    /// <exception cref="DataNotInitialized">If the caller attempts to utilize this library without creating an instance of this class.</exception>
    public static T GetEntity<T>(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetEntity<T>(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Retrieves a row from the database and converts to the specified entity type.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">Type of SQL command to execute. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>An entity.</returns>
    /// <exception cref="DataNotInitialized">If the caller attempts to utilize this library without creating an instance of this class.</exception>
    public static T GetEntity<T>(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, parameters, connectionName, dbCommandType).ToEntity<T>();

    /// <summary>
    /// Retrieves a table from the database and converts to a <see cref="IList"/>/<T/> of the specified entity type.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="dbCommandType">Type of SQL command to execute. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>A <see cref="IList"/>/<T/> of entities</returns>
    /// <exception cref="DataNotInitialized">If the caller attempts to utilize this library without creating an instance of this class.</exception>
    public static IList<T> GetEntityList<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, dbCommandType).ToEntities<T>().ToList();

    /// <summary>
    /// Retrieves a table from the database and converts to a <see cref="IList"/>/<T/> of the specified entity type.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">Type of SQL command to execute. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>A <see cref="IList"/>/<T/> of entities</returns>
    /// <exception cref="DataNotInitialized">If the caller attempts to utilize this library without creating an instance of this class.</exception>
    public static IList<T> GetEntityList<T>(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, parameters, dbCommandType).ToEntities<T>().ToList();

    /// <summary>
    /// Retrieves a table from the database and converts to a <see cref="IList"/>/<T/> of the specified entity type.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">Type of SQL command to execute. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>A <see cref="IList"/>/<T/> of entities</returns>
    /// <exception cref="DataNotInitialized">If the caller attempts to utilize this library without creating an instance of this class.</exception>
    public static IList<T> GetEntityList<T>(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, parameters, connectionName, dbCommandType).ToEntities<T>().ToList();
    
    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataSet GetDataSet(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataSet(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataSet GetDataSet(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataSet(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataSet GetDataSet(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return FillDataSet(command, parameters, connectionName, dbCommandType).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Execute the provided command using the default connection and return the results in a DataTable.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Execute the provided command using the default connection and return the results in a DataTable.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="command">Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTable(command, parameters, connectionName, 0, dbCommandType);

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="tableIndex">Index within tables collection to return</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DataTable GetDataTable(string command, Parameters? parameters, string connectionName, int tableIndex, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return FillDataSet(command, parameters, connectionName, dbCommandType).GetAwaiter().GetResult().Tables[tableIndex];
    }


    /// <summary>
    /// Executes the provided command using the default connection and returns an open DataReader.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader GetDataReader(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataReader(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Executes the provided command using the default connection and returns an open <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader GetDataReader(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataReader(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Executes the provided command using the default connection and returns an open DataReader.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    public static DbDataReader GetDataReader(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        
        return CreateDataReader(command, parameters, connectionName, dbCommandType).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => Execute(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => Execute(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

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
    public static int Execute(string command, Parameters? parameters, string connectionName = "default", 
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return ExecuteInternal(command, parameters, connectionName, dbCommandType).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Executes the provided command and returns a single value.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalar(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalar(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

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
    public static object ExecuteScalar(string command, Parameters? parameters, string connectionName = "default", 
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return ExecuteScalarInternal(command, parameters, connectionName, dbCommandType).GetAwaiter().GetResult();
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
    public static T ExecuteScalar<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalar<T>(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

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
    public static T ExecuteScalar<T>(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalar<T>(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

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
    public static T ExecuteScalar<T>(string command, Parameters? parameters, string connectionName = "default", 
        CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return ExecuteScalarInternal<T>(command, parameters, connectionName, dbCommandType).GetAwaiter().GetResult();
    }
    
    #region Public Asynchronous API (recommended for new code)

    //TODO: Add XML comments
    public static Task<T> GetEntityAsync<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetEntityAsync<T>(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<T> GetEntityAsync<T>(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetEntityAsync<T>(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<T> GetEntityAsync<T>(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
        => (await GetDataTableAsync(command, parameters, connectionName, dbCommandType)).ToEntity<T>();

    public static Task<IList<T>> GetEntityListAsync<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetEntityListAsync<T>(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<IList<T>> GetEntityListAsync<T>(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetEntityListAsync<T>(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<IList<T>> GetEntityListAsync<T>(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
        => (await GetDataTableAsync(command, parameters, connectionName, dbCommandType)).ToEntities<T>().ToList();

    public static Task<DataSet> GetDataSetAsync(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataSetAsync(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<DataSet> GetDataSetAsync(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataSetAsync(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<DataSet> GetDataSetAsync(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return await FillDataSet(command, parameters, connectionName, dbCommandType);
    }

    public static Task<DataTable> GetDataTableAsync(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTableAsync(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<DataTable> GetDataTableAsync(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataTableAsync(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<DataTable> GetDataTableAsync(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
        => (await GetDataSetAsync(command, parameters, connectionName, dbCommandType)).Tables[0];

    public static Task<DbDataReader> GetDataReaderAsync(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataReaderAsync(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<DbDataReader> GetDataReaderAsync(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => GetDataReaderAsync(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<DbDataReader> GetDataReaderAsync(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return await CreateDataReader(command, parameters, connectionName, dbCommandType);
    }

    public static Task<int> ExecuteAsync(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteAsync(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<int> ExecuteAsync(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteAsync(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<int> ExecuteAsync(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return await ExecuteInternal(command, parameters, connectionName, dbCommandType);
    }

    public static Task<object> ExecuteScalarAsync(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalarAsync(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<object> ExecuteScalarAsync(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalarAsync(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<object> ExecuteScalarAsync(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        return await ExecuteScalarInternal(command, parameters, connectionName, dbCommandType);
    }

    public static Task<T> ExecuteScalarAsync<T>(string command, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalarAsync<T>(command, null, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static Task<T> ExecuteScalarAsync<T>(string command, Parameters? parameters, CommandType dbCommandType = CommandType.StoredProcedure)
        => ExecuteScalarAsync<T>(command, parameters, Environment.Connections.DefaultConnection.ConnectionName, dbCommandType);

    public static async Task<T> ExecuteScalarAsync<T>(string command, Parameters? parameters, string connectionName, CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        var result = await ExecuteScalarInternal(command, parameters, connectionName, dbCommandType);
        return (T)result!;
    }

    #endregion
    
    #region Private Helpers

    /// <summary>
    /// Returns a filled dataset
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameters</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    /// <returns/>
    private static async Task<DataSet> FillDataSet(string command, IEnumerable<DbParameter>? parameters,
        string connectionName = "default", CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        var connProps = Environment.Connections[connectionName];
        var dataSet = new DataSet();

        await using var connection = connProps.DbServerType == DbServerType.postgresql
            ? (DbConnection)new NpgsqlConnection(connProps.ConnectionString)
            : new SqlConnection(connProps.ConnectionString);

        await connection.OpenAsync();

        await using var cmd = CreateCommand(command, connection, dbCommandType, parameters, connProps.DbServerType);

        var adapter = connProps.DbServerType == DbServerType.postgresql
            ? (DbDataAdapter)new NpgsqlDataAdapter((NpgsqlCommand)cmd)
            : new SqlDataAdapter((SqlCommand)cmd);

        adapter.Fill(dataSet);
        return dataSet;
    }

    private static DbCommand CreateCommand(string commandText, DbConnection connection, CommandType commandType,
        IEnumerable<DbParameter>? parameters, DbServerType serverType)
    {
        DbCommand cmd = serverType == DbServerType.postgresql
            ? new NpgsqlCommand(commandText, (NpgsqlConnection)connection)
            : new SqlCommand(commandText, (SqlConnection)connection);

        cmd.CommandType = commandType;
        cmd.CommandTimeout = CommandTimeout;

        if (parameters != null)
        {
            foreach (var p in parameters)
            {
                cmd.Parameters.Add(p);
            }
        }

        return cmd;
    }

    /// <summary>
    /// Executes the provided command and returns an open DataReader.
    /// </summary>
    /// <param name="command">SQL Command to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="dbCommandType">SQL command type. Defaults to <see cref="CommandType.StoredProcedure"/>.</param>
    private static async Task<DbDataReader> CreateDataReader(string command, Parameters? parameters,
        string connectionName = "default", CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        var connProps = Environment.Connections[connectionName];

        await using var connection = connProps.DbServerType == DbServerType.postgresql
            ? (DbConnection)new NpgsqlConnection(connProps.ConnectionString)
            : new SqlConnection(connProps.ConnectionString);

        await connection.OpenAsync();

        await using var cmd = CreateCommand(command, connection, dbCommandType, parameters, connProps.DbServerType);

        return await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
    }

    private static async Task<int> ExecuteInternal(string command, Parameters? parameters,
        string connectionName = "default", CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        var connProps = Environment.Connections[connectionName];

        await using var connection = connProps.DbServerType == DbServerType.postgresql
            ? (DbConnection)new NpgsqlConnection(connProps.ConnectionString)
            : new SqlConnection(connProps.ConnectionString);

        await connection.OpenAsync();

        await using var cmd = CreateCommand(command, connection, dbCommandType, parameters, connProps.DbServerType);

        DbParameter returnParameter = connProps.DbServerType == DbServerType.postgresql
            ? new NpgsqlParameter("p_out", NpgsqlDbType.Integer) { Direction = ParameterDirection.Output }
            : new SqlParameter("@RETURN_VALUE", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue };

        cmd.Parameters.Add(returnParameter);

        await cmd.ExecuteNonQueryAsync();

        return Convert.ToInt32(returnParameter.Value);
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
    private static async Task<object> ExecuteScalarInternal(string command, Parameters? parameters,
        string connectionName = "default", CommandType dbCommandType = CommandType.StoredProcedure)
    {
        ValidateConnection(connectionName);
        var connProps = Environment.Connections[connectionName];

        await using var connection = connProps.DbServerType == DbServerType.postgresql
            ? (DbConnection)new NpgsqlConnection(connProps.ConnectionString)
            : new SqlConnection(connProps.ConnectionString);

        await connection.OpenAsync();

        await using var cmd = CreateCommand(command, connection, dbCommandType, parameters, connProps.DbServerType);

        var result = await cmd.ExecuteScalarAsync();
        return result ?? DBNull.Value;
    }

    private static async Task<T> ExecuteScalarInternal<T>(string command, Parameters? parameters,
        string connectionName = "default", CommandType dbCommandType = CommandType.StoredProcedure)
    {
        var result = await ExecuteScalarInternal(command, parameters, connectionName, dbCommandType);
        return result is DBNull or null ? default! : (T)result;
    }

    /// <summary>
    /// Validates that given connections keys/names are valid
    /// </summary>
    /// <param name="connectionName">Name of connection properties stored internally.</param>
    private static void ValidateConnection(string connectionName)
    {
        if (string.IsNullOrEmpty(connectionName))
            throw new UnknownConnection("Connection key/name cannot be empty.");

        if (!Environment.Connections[connectionName].IsValid())
            throw new UnknownConnection($"Unknown connection name: '{connectionName}'");
    }

    #endregion
}