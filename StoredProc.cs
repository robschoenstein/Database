using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Database
{
  public static class StoredProc
  {
    private const int COMMAND_TIMEOUT = 180;

    //TODO: ExpandoObject as parameters
    //TODO: Eliminate the need for "dbo." preceding a user defined type
    //TODO: Simplify user defined type parameter declaration

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    public static DataSet GetDataSet(string procedure)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataSet(procedure, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    public static DataSet GetDataSet(string procedure, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataSet(procedure, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataSet.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    public static DataSet GetDataSet(string procedure, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);
      return FillDatSet(procedure, parameters, connectionName);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    public static DataTable GetDataTable(string procedure)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataTable(procedure, null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    public static DataTable GetDataTable(string procedure, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataTable(procedure, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    public static DataTable GetDataTable(string procedure, Parameters parameters, string connectionName)
    {
      return GetDataTable(procedure, parameters, connectionName, 0);
    }

    /// <summary>
    /// Execute the provided procedure and return the results in a DataTable.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <param name="tableIndex">Index within tables collection to return</param>
    public static DataTable GetDataTable(string procedure, Parameters parameters, string connectionName, int tableIndex)
    {
      ValidateConnection(connectionName);
      return FillDatSet(procedure, parameters, connectionName).Tables[tableIndex];
    }

    /// <summary>
    /// Executes the provided procedure and returns an open DataReader.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    public static SqlDataReader GetDataReader(string procedure)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataReader(procedure, null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure and returns an open DataReader.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    public static SqlDataReader GetDataReader(string procedure, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return GetDataReader(procedure, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure and returns an open DataReader.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    public static SqlDataReader GetDataReader(string procedure, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(procedure, new SqlConnection(Environment.Connections[connectionName]))
        {
          CommandType = CommandType.StoredProcedure,
          CommandTimeout = 180
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
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string procedure)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return Execute(procedure, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string procedure, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return Execute(procedure, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns>
    /// Stored procedure RETURN value.
    /// </returns>
    public static int Execute(string procedure, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(procedure, new SqlConnection(Environment.Connections[connectionName]))
        {
          CommandType = CommandType.StoredProcedure,
          CommandTimeout = COMMAND_TIMEOUT
        };

      if (parameters != null)
      {
        foreach (var sqlParameter in parameters)
          sqlCommand.Parameters.Add(sqlParameter);
      }

      var sqlParameter1 = new SqlParameter("@RETURN_VALUE", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue };
      sqlCommand.Parameters.Add(sqlParameter1);

      if (sqlCommand.Connection.State != ConnectionState.Open)
        sqlCommand.Connection.Open();

      sqlCommand.ExecuteNonQuery();
      sqlCommand.Connection.Close();

      return Convert.ToInt32(sqlParameter1.Value);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string procedure)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar(procedure, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string procedure, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar(procedure, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    public static object ExecuteScalar(string procedure, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(procedure, new SqlConnection(Environment.Connections[connectionName]))
        {
          CommandType = CommandType.StoredProcedure,
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
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <returns>
    /// Object containing the result
    /// </returns>
    /// <exception cref="DefaultConnectionNotDefined">The Default Connection has not been defined. Please define prior to use.</exception>
    public static T ExecuteScalar<T>(string procedure)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar<T>(procedure, (Parameters)null, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <returns>
    /// Result as T
    /// </returns>
    /// <exception cref="DefaultConnectionNotDefined">The Default Connection has not been defined. Please define prior to use.</exception>
    public static T ExecuteScalar<T>(string procedure, Parameters parameters)
    {
      if (Environment.Connections.DefaultConnection == string.Empty)
        throw new DefaultConnectionNotDefined("The Default Connection has not been defined. Please define prior to use.");
      else
        return ExecuteScalar<T>(procedure, parameters, Environment.Connections.DefaultConnection);
    }

    /// <summary>
    /// Executes the provided procedure and returns a single value.
    /// </summary>
    /// <typeparam name="T">Return Type</typeparam>
    /// <param name="procedure">Stored Procedure to execute</param>
    /// <param name="parameters">Parameter list</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns>
    /// Result as T
    /// </returns>
    public static T ExecuteScalar<T>(string procedure, Parameters parameters, string connectionName)
    {
      ValidateConnection(connectionName);

      var sqlCommand = new SqlCommand(procedure, new SqlConnection(Environment.Connections[connectionName]))
      {
        CommandType = CommandType.StoredProcedure,
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
    /// <param name="procedure">Name of stored procedure to execute</param>
    /// <param name="parameters">Parameters</param>
    /// <param name="connectionName">Connection key/name</param>
    /// <returns/>
    private static DataSet FillDatSet(string procedure, IEnumerable<SqlParameter> parameters, string connectionName)
    {
      var selectCommand = new SqlCommand(procedure, new SqlConnection(Environment.Connections[connectionName]))
      {
        CommandType = CommandType.StoredProcedure,
        CommandTimeout = COMMAND_TIMEOUT
      };

      if (parameters != null)
      {
        foreach (var sqlParameter in parameters)
          selectCommand.Parameters.Add(sqlParameter);
      }

      var sqlDataAdapter = new SqlDataAdapter(selectCommand);
      var dataSet = new DataSet();
      sqlDataAdapter.Fill(dataSet);
      selectCommand.Connection.Close();
      return dataSet;
    }
  }
}
