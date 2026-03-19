using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Database.Entity;
using Database.Entity.Attributes;
using Database.Enums;
using Database.Exceptions;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;

namespace Database
{
    /// <summary>
    /// Parameters collection for executing stored procedures and other commands that require parameters
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    public class Parameters : IEnumerable<DbParameter>, IDisposable
    {
        // ReSharper disable once UseCollectionExpression
        // ReSharper disable once HeapView.ObjectAllocation.Evident
        // ReSharper disable once RedundantEmptyObjectOrCollectionInitializer
        private List<DbParameter> _parameters = new() { };

        public DbParameter this[int index]
        {
            get => _parameters[index];
            set
            {
                if (index >= _parameters.Count)
                {
                    throw new IndexOutOfRangeException();
                }

                _parameters[index] = value;
            }
        }

        /// <summary>
        /// Returns the number of parameters in the list.
        /// </summary>
        public int Count
        {
            get { return _parameters.Count; }
        }

        public Parameters()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class</param>
        public Parameters(string parameterName, object value, string connectionName = "default")
        {
            if (!Environment.Initialized)
            {
                throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
            }
            
            Add(parameterName, value, connectionName);
        }

        /// <summary>
        /// Finds a parameter matching the provided name.
        /// </summary>
        /// <param name="parameterName">Name of the parameter to find.</param>
        /// <returns>
        /// If found, returns the value of the index (positive integer). Otherwise, -1 is returned.
        /// </returns>
        public int FindParameter(string parameterName)
        {
            for (var index = 0; index < _parameters.Count; ++index)
            {
                if (_parameters[index].ParameterName == parameterName)
                {
                    return index;
                }
            }

            return -1;
        }

        /// <summary>
        /// Builds an XML string from list of values.
        /// </summary>
        /// <typeparam name="T">Type of values</typeparam>
        /// <param name="xmlRootName">root name item</param>
        /// <param name="item">identity of item</param>
        /// <param name="values">List of values</param>
        /// <returns>Returns a string in XML format to utilize in SQL.</returns>
        public static string BuildXmlString<T>(string xmlRootName, string item, List<T> values)
        {
            if (!typeof(T).IsSqlConvertableType())
            {
                return null;
            }

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendFormat("<{0}>", xmlRootName);

            foreach (var t in values)
            {
                stringBuilder.AppendFormat("<" + item + ">{0}</" + item + ">", t);
            }

            stringBuilder.AppendFormat("</{0}>", xmlRootName);

            return (stringBuilder).ToString();
        }

        /// <summary>
        /// Adds the given parameter to the list of parameters
        /// </summary>
        /// <param name="parameter">Parameter to add</param>
        /// <returns>
        /// True if the parameter was added. False if a parameter with the same name already
        ///             existed in the parameter list
        /// </returns>
        public bool Add(DbParameter parameter)
        {
            if (FindParameter(parameter.ParameterName) > 0)
            {
                return false;
            }

            _parameters.Add(parameter);

            return true;
        }

        /// <summary>
        /// Creates a new parameter with the given name and value then adds it to 
        /// the list of parameters.
        /// </summary>
        /// <param name="parameterName">Name of the parameter</param><param name="value">Value of the parameter</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class</param>
        /// <returns>
        /// True if the parameter was added. False if a parameter with the same name already
        ///             existed in the parameter list
        /// </returns>
        // ReSharper disable once MemberCanBePrivate.Global
        public bool Add(string parameterName, object value, string connectionName = "default")
        {
            if (!Environment.Initialized)
            {
                throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
            }
            
            if (FindParameter(parameterName) > 0)
            {
                return false;
            }

            switch (Environment.Connections[connectionName].DbServerType)
            {
                case DbServerType.postgresql:
                    _parameters.Add(new NpgsqlParameter(parameterName, value));
                    break;
                case DbServerType.mssql:
                    _parameters.Add(new SqlParameter(parameterName, value));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        /// <summary>
        /// Adds a table valued parameter of the specified type
        /// </summary>
        /// <typeparam name="T">Type contained in collection</typeparam>
        /// <param name="parameterName">Name of the parameter.</param>
        /// <param name="values">Collection of values.</param>
        /// <param name="typeName">Table valued parameter name (eg. dbo.SimpleValueType).</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class</param>
        /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
        public bool Add<T>(string parameterName, IList<T> values, string typeName, string connectionName = "default")
        {
            if (!Environment.Initialized)
            {
                throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
            }
            
            if (!values.Any())
            {
                return false;
            }

            var defaultSchema = Environment.Connections[connectionName].DefaultDbSchema;

            //if the typeName does not already contain the default schema name for the selected connection
            if (!typeName.StartsWith($"{defaultSchema}."))
            {
                //if the typeName does not already contain a schema name. The user may want to utilize a different schema
                if (!typeName.Contains('.'))
                {
                    //prepend the default schema name for the selected connection
                    typeName = $"{defaultSchema}.{typeName}";
                }
            }

            return typeof(T).IsSqlConvertableType()
                ? AddSimpleTVP(parameterName, values, typeName, connectionName)
                : AddComplexTVP(parameterName, values, typeName, connectionName);
        }

        /// <summary>
        /// Adds a Table Valued Parameter (TVP)
        /// </summary>
        /// <param name="parameterName">Name of the parameter</param>
        /// <param name="table">DataTable containing data</param>
        /// <param name="typeName">Table valued parameter name (eg. dbo.SimpleValueType) Microslop SQL Server only. Not used for PostgreSql, so it defaults to null.</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class</param>
        /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
        public bool Add(string parameterName, DataTable table, string typeName = null, string connectionName = "default")
        {
            if (!Environment.Initialized)
            {
                throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
            }
            
            if (table == null || table.Rows.Count == 0)
            {
                return false;
            }
            
            switch (Environment.Connections[connectionName].DbServerType)
            {
                case DbServerType.postgresql:
                    //PostgreSql does not support using DataTables as parameters. It only supports json, so we need to convert the DataTable to json.
                    var data = table.Rows.OfType<DataRow>()
                        .Select(row => table.Columns.OfType<DataColumn>()
                            .ToDictionary(col => col.ColumnName, col => row[col]));
                    
                    var json = JsonSerializer.Serialize(data, new JsonSerializerOptions{ WriteIndented = true });
                    
                    var npgsqlParameter = new NpgsqlParameter(parameterName, json)
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    };
                    
                    _parameters.Add(npgsqlParameter);
                    break;
                case DbServerType.mssql:
                    var sqlParameter = new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    };
                    
                    _parameters.Add(sqlParameter);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            return true;
        }

        /// <summary>
        /// Creates a new parameter with the given name, database type, and value 
        /// then adds it to the list of parameters.
        /// </summary>
        /// <param name="parameterName">Name of the parameter</param>
        /// <param name="type">Type of the parameter</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class.</param>
        /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
        public bool Add(string parameterName, DbType type, object value, string connectionName = "default")
        {
            if (!Environment.Initialized)
            {
                throw new DataNotInitialized("The Data class has not been initialized. Please call Data.Initialize(ConnectionProperties) and supply the default connection properties.");
            }
            
            if (FindParameter(parameterName) > 0)
            {
                return false;
            }

            switch (Environment.Connections[connectionName].DbServerType)
            {
                case DbServerType.postgresql:
                    _parameters.Add(new NpgsqlParameter(parameterName, value)
                    {
                        DbType = type
                    });
                    
                    break;
                case DbServerType.mssql:
                    _parameters.Add(new SqlParameter(parameterName, value)
                    {
                        DbType = type
                    });
                    
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        /// <summary>
        /// Removes a parameter with the provided name from the parameter list.
        /// </summary>
        /// <param name="parameterName">Name of the parameter to remove.</param>
        /// <returns><c>true</c> if parameter was removed, <c>false</c> otherwise.</returns>
        public bool Remove(string parameterName)
        {
            var parameter = FindParameter(parameterName);

            if (parameter <= -1)
            {
                return false;
            }

            _parameters.RemoveAt(parameter);

            return true;
        }

        /// <summary>
        /// Removes the parameter at the provided index.
        /// </summary>
        /// <param name="index">Index of the parameter to remove</param>
        public void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        /// <summary>
        /// Clear all parameters from the parameter list.
        /// </summary>
        public void Clear()
        {
            _parameters.Clear();
        }

        public IEnumerator<DbParameter> GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        public void Dispose()
        {
            _parameters = null;
        }

        /// <summary>
        /// Creates a new table valued parameter (primitive or SQL mappable type (eg. int, string, DateTime, Guid)) 
        ///  with the given name, <see cref="IEnumerable{T}"/> value, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <typeparam name="T">Enumerable type</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Enumerable value.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool AddSimpleTVP<T>(
            string parameterName, IEnumerable<T> value, string typeName, string connectionName = "default")
        {
            if (FindParameter(parameterName) > 0)
            {
                return false;
            }

            if (!typeof(T).IsSqlConvertableType())
            {
                throw new ArgumentException(
                    "value parameter must be an IEnumerable<T> of a primitive or SQL mappable type (eg. int, string, DateTime, Guid).",
                    nameof(value));
            }

            switch (Environment.Connections[connectionName].DbServerType)
            {
                case DbServerType.postgresql:
                    //PostgreSQL does not support table valued parameters. So, we pass it as JSON and process it in
                    //  a stored procedure or function.
                    var inputList = value.ToList();

                    var dataDictionary = inputList.ToDictionary(_ => "value", data => data.ToString());

                    var json = JsonSerializer.Serialize(dataDictionary, new JsonSerializerOptions{ WriteIndented = true });
                    
                    var npgsqlParameter = new NpgsqlParameter(parameterName, json)
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    };
                    
                    _parameters.Add(npgsqlParameter);
                    break;
                case DbServerType.mssql:
                    var table = new DataTable();

                    table.Columns.Add("value", typeof(string));

                    foreach (var item in value)
                    {
                        table.Rows.Add(item.ToString());
                    }
                    
                    var sqlParameter = new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    };
                    
                    _parameters.Add(sqlParameter);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            return true;
        }

        /// <summary>
        /// Creates a new table valued parameter (containing a complex type that cannot be converted directly to SQL type)
        ///  with the given name, <see cref="IList{T}"/> value, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <typeparam name="T">Type in the Enumerator</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Enumerable value.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool AddComplexTVP<T>(
            string parameterName, IList<T> value,
            string typeName, string connectionName = "default")
        {
            if (FindParameter(parameterName) > 0)
            {
                return false;
            }

            if (typeof(T).IsSqlConvertableType())
            {
                throw new ArgumentException(
                    "value parameter must be an IEnumerable<T> of a complex type (eg. object consisting of multiple properties).",
                    nameof(value));
            }


            if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(typeof(T)))
            {
                throw new ArgumentException(
                    "value parameter cannot contain dynamic objects... yet.",
                    nameof(value));
            }

            return CreateComplexTVP(parameterName, value, typeName, connectionName);
        }

        /// <summary>
        /// Creates a new table valued parameter (containing a complex type that cannot be converted directly to SQL type)
        ///  with the given name, <see cref="IEnumerable{T}"/> value, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <typeparam name="T">Type in the Enumerator</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Enumerable value.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool CreateComplexTVP<T>(string parameterName, IEnumerable<T> value, string typeName,
            string connectionName = "default")
        {
            //Converting IList<T> to a DataTable with property names (or TVPColumnMapping names) as column names and
            //  each entity as a row. I really need to do this for only MSSQL and create a different approach for other
            //  databases
            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttributes(typeof(TVPIgnore), false).Length < 1)
                .ToList();
            
            switch (Environment.Connections[connectionName].DbServerType)
            {
                case DbServerType.postgresql:
                    //PostgreSQL does not support table valued parameters. So, we pass it as JSON and process it in
                    //  a stored procedure or function.
                    var npgsqlParameter = new NpgsqlParameter(parameterName, EntityListToJson(value.ToList()))
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    };
                    
                    _parameters.Add(npgsqlParameter);
                    break;
                case DbServerType.mssql:
                    var table = new DataTable();
                    var selectedProps = new ConcurrentBag<PropertyInfo>();
                    var columns = new ConcurrentDictionary<int, DataColumn>();

                    var cancelTokenSource = new CancellationTokenSource();

                    var options = new ParallelOptions
                    {
                        CancellationToken = cancelTokenSource.Token,
                        MaxDegreeOfParallelism = 2
                    };

                    Parallel.For(0, properties.Count(), options, i =>
                    {
                        var attr = (TVPColumn[])properties[i].GetCustomAttributes(typeof(TVPColumn), false);

                        var colName = attr.Length > 0 && !string.IsNullOrEmpty(attr[0].ColumnName)
                            ? attr[0].ColumnName
                            : properties[i].Name;

                        columns.TryAdd(i, new DataColumn(colName, ProcessColumnType(properties[i].PropertyType)));
                        selectedProps.Add(properties[i]);
                    });

                    table.Columns.AddRange(columns.OrderBy(c => c.Key).Select(c => c.Value).ToArray());

                    foreach (var item in value)
                    {
                        var row = table.NewRow();

                        foreach (var prop in selectedProps)
                        {
                            row[prop.Name] = prop.GetValue(item, null) ?? DBNull.Value;
                        }

                        table.Rows.Add(row);
                    }
                    
                    var sqlParameter = new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    };
                    
                    _parameters.Add(sqlParameter);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        /// <summary>
        /// Creates a new table valued parameter (containing a complex type that cannot be converted directly to SQL type)
        ///  with the given name, <see cref="IList{T}"/> value, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <typeparam name="T">Type in the Enumerator</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Enumerable value.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type. Required by Microslop SQL Server. Not Required by PostgreSQL.</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the Data class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        // ReSharper disable once UnusedMember.Local
        private bool CreateComplexTVPFromExpando<T>(string parameterName, IList<T> value,
            string typeName = null, string connectionName = "default")
        {
            //Converting IList<T> to a DataTable with property names (or TVPColumnMapping names) as column names and
            //  each entity as a row. I really need to do this for only MSSQL and create a different approach for other
            //  databases
            

            switch (Environment.Connections[connectionName].DbServerType)
            {
                case DbServerType.postgresql:
                    //PostgreSQL does not support table valued parameters. So, we pass it as JSON and process it in
                    //  a stored procedure or function.
                    var npgsqlParameter = new NpgsqlParameter(parameterName, EntityListToJson(value))
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    };
                    
                    _parameters.Add(npgsqlParameter);
                    break;
                case DbServerType.mssql:
                    var table = new DataTable();

                    table.Columns.AddRange(value
                        .Cast<IDictionary<string, object>>()
                        .First()
                        .Select(v => v.Value != null
                            ? new DataColumn(v.Key, ProcessColumnType(v.Value.GetType()))
                            : new DataColumn(v.Key)).ToArray());

                    foreach (var item in value)
                    {
                        var row = table.NewRow();

                        foreach (var prop in (IDictionary<string, object>)item)
                        {
                            row[prop.Key] = prop.Value ?? DBNull.Value;
                        }

                        table.Rows.Add(row);
                    }
                    
                    var sqlParameter = new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    };
                    
                    _parameters.Add(sqlParameter);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        private static string EntityListToJson<T>(IList<T> value)
        {
            var data = new Dictionary<string, object>();
                    
            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttributes(typeof(TVPIgnore), false).Length < 1)
                .ToList();
                    
            for (int i = 0; i < value.Count(); i++)
            {
                var attr = (TVPColumn[])properties[i].GetCustomAttributes(typeof(TVPColumn), false);

                var propName = attr.Length > 0 && !string.IsNullOrEmpty(attr[0].ColumnName)
                    ? attr[0].ColumnName
                    : properties[i].Name;

                var propValue = properties[i].GetValue(value);
                        
                data.Add(propName, propValue);
            }
                    
            return JsonSerializer.Serialize(data, new JsonSerializerOptions{ WriteIndented = true });
        }

        /// <summary>
        /// Retrieves a type that is compatable with a <see cref="DataColumn"/>.
        /// </summary>
        /// <remarks>Basically checks for <see cref="Nullable"/> and gets the underlying type if it is.</remarks>
        /// <param name="type">The type.</param>
        /// <returns>Compatable type.</returns>
        private static Type ProcessColumnType(Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);

            if (underlyingType != null)
            {
                type = underlyingType;
            }

            return type;
        }
    }
}