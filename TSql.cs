using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Database
{
  public static class TSql
  {
    private const int COMMAND_TIMEOUT = 0;

    /// <summary>
    /// Execute the provided sql and return the results in a DataSet.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    public static DataSet GetDataSet(string sql)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataSet(sql, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided sql and return the results in a DataSet.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    public static DataSet GetDataSet(string sql, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataSet(sql, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided sql and return the results in a DataSet.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    public static DataSet GetDataSet(string sql, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);
      return FillDatSet(sql, parameters, connectionName);
    }

    /// <summary>
    /// Execute the provided sql and return the results in a DataTable.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    public static DataTable GetDataTable(string sql)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataTable(sql, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided sql and return the results in a DataTable.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    public static DataTable GetDataTable(string sql, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataTable(sql, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided sql and return the results in a DataTable.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    public static DataTable GetDataTable(string sql, Parameters parameters, string connectionName)
    {
      return GetDataTable(sql, parameters, connectionName, 0);
    }

    /// <summary>
    /// Execute the provided sql and return the results in a DataTable.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="tableIndex">Index within tables collection to return</param>
    public static DataTable GetDataTable(string sql, Parameters parameters, string connectionName, int tableIndex)
    {
      ValidateConnection(connectionName);
      return FillDatSet(sql, parameters, connectionName).Tables[tableIndex];
    }

    /// <summary>
    /// Executes the provided sql and returns an open DataReader.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    public static SqlDataReader GetDataReader(string sql)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataReader(sql, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql and returns an open DataReader.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    public static SqlDataReader GetDataReader(string sql, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataReader(sql, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql and returns an open DataReader.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    public static SqlDataReader GetDataReader(string sql, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(sql,
                                        new SqlConnection(Environment.Connections[connectionName]))
        {
          CommandTimeout = COMMAND_TIMEOUT
        };

      if (parameters != null)
      {
        foreach (var sqlParameter in parameters)
          sqlCommand.Parameters.Add(sqlParameter);
      }

      if (sqlCommand.Connection.State != ConnectionState.Open)
        sqlCommand.Connection.Open();

      return sqlCommand.ExecuteReader();
    }

    /// <summary>
    /// Executes the provided sql.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <returns>
    /// Stored sql RETURN value.
    /// </returns>
    public static int Execute(string sql)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return Execute(sql, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <returns>
    /// Stored sql RETURN value.
    /// </returns>
    public static int Execute(string sql, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return Execute(sql, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns>
    /// Stored sql RETURN value.
    /// </returns>
    public static int Execute(string sql, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(sql,
                                        new SqlConnection(Environment.Connections[connectionName]))
        {
          CommandTimeout = COMMAND_TIMEOUT
        };

      if (parameters != null)
      {
        foreach (var sqlParameter in parameters)
          sqlCommand.Parameters.Add(sqlParameter);
      }

      //var sqlParameter1 = new SqlCeParameter("@RETURN_VALUE", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue };

      //sqlCommand.Parameters.Add(sqlParameter1);

      if (sqlCommand.Connection.State != ConnectionState.Open)
        sqlCommand.Connection.Open();

      sqlCommand.ExecuteNonQuery();
      sqlCommand.Connection.Close();

      return Convert.ToInt32(0);//sqlParameter1.Value);
    }

    /// <summary>
    /// Executes the provided sql and returns a single value.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string sql)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar(sql, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql and returns a single value.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string sql, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar(sql, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql and returns a single value.
    /// </summary>
    /// <param name="sql">SQL to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string sql, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(sql,
                                        new SqlConnection(Environment.Connections[connectionName]))
        {
          CommandTimeout = COMMAND_TIMEOUT
        };

      if (parameters != null)
      {
        foreach (var sqlParameter in parameters)
          sqlCommand.Parameters.Add(sqlParameter);
      }

      if (sqlCommand.Connection.State != ConnectionState.Open)
        sqlCommand.Connection.Open();

      var obj = sqlCommand.ExecuteScalar();

      sqlCommand.Connection.Close();

      return obj;
    }

    /// <summary>
    /// Executes the provided sql and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="sql">SQL to execute.</param>
    /// <returns>
    /// Result as T
    /// </returns>
    /// <exception cref="DefaultConnectionNotDefined">The Default Connection has not been defined. Please define prior to use.</exception>
    public static T ExecuteScalar<T>(string sql)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar<T>(sql, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="sql">The sql.</param>
    /// <param name="parameters">Parameter list</param>
    /// <returns>
    /// Result as T
    /// </returns>
    /// <exception cref="DefaultConnectionNotDefined">The Default Connection has not been defined. Please define prior to use.</exception>
    public static T ExecuteScalar<T>(string sql, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar<T>(sql, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided sql and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="sql">The sql.</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns>
    /// Result as T
    /// </returns>
    public static T ExecuteScalar<T>(string sql, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(sql,
                                        new SqlConnection(Environment.Connections[connectionName]))
      {
        CommandTimeout = COMMAND_TIMEOUT
      };

      if (parameters != null)
      {
        foreach (var sqlParameter in parameters)
          sqlCommand.Parameters.Add(sqlParameter);
      }

      if (sqlCommand.Connection.State != ConnectionState.Open)
        sqlCommand.Connection.Open();

      var obj = sqlCommand.ExecuteScalar();

      sqlCommand.Connection.Close();

      return (T)obj;
    }

    /// <summary>
    /// Validates that given connections keys/names are valid
    /// 
    /// </summary>
    /// <param name="connection"/>
    private static void ValidateConnection(string connection)
    {
      if (connection == string.Empty)
        throw new UnknownConnection("Connection key/name cannot be empty.");
      if (Environment.Connections[connection] == string.Empty)
        throw new UnknownConnection("Connection key/name '" + connection + "' refers to an unknown connection.");
    }

    /// <summary>
    /// Returns a filled dataset
    /// </summary>
    /// <param name="sql">The sql.</param>
    /// <param name="parameters">Parameters</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns></returns>
    private static DataSet FillDatSet(string sql, IEnumerable<SqlParameter> parameters, string connectionName)
    {
      var selectCommand = new SqlCommand(sql,
                                     new SqlConnection(Environment.Connections[connectionName]))
      {
        CommandTimeout = COMMAND_TIMEOUT
      };

      var dataSet = new DataSet();

      try
      {
        if (parameters != null)
        {
          foreach (var sqlParameter in parameters)
            selectCommand.Parameters.Add(sqlParameter);
        }

        var sqlDataAdapter = new SqlDataAdapter(selectCommand);

        sqlDataAdapter.Fill(dataSet);
      }
      finally
      {
        selectCommand.Parameters.Clear();
        selectCommand.Connection.Close();
      }


      return dataSet;
    }
  }
}
