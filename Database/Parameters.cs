using System.Collections;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Database.Attributes;
using Database.Dynamic;
using Database.Entity;
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
    /// <remarks>
    /// Supports MSSQL Table Valued Parameters (TVPs) and PostgreSQL JSONb falback. Thread-safe.
    /// </remarks>
    public class Parameters : IEnumerable<DbParameter>, IDisposable
    {
        private List<DbParameter> _parameters = new() { };

        public DbParameter this[int index]
        {
            get => _parameters[index];
            set => _parameters[index] = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Returns the number of parameters in the list.
        /// </summary>
        public int Count
        {
            get => _parameters.Count;
        }

        public Parameters()
        {
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class</param>
        public Parameters(string parameterName, object value, string connectionName = "default")
        {
            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("The DataAccess class has not been initialized.");
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
            for (var i = 0; i < _parameters.Count; ++i)
            {
                if (_parameters[i].ParameterName == parameterName)
                {
                    return i;
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
                return null!;
            }

            var sb = new StringBuilder();

            sb.AppendFormat("<{0}>", xmlRootName);

            foreach (var t in values)
            {
                sb.AppendFormat("<{0}>{1}</{0}>", item, t);
            }

            sb.AppendFormat("</{0}>", xmlRootName);

            return sb.ToString();
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
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class</param>
        /// <returns>
        /// True if the parameter was added. False if a parameter with the same name already
        ///             existed in the parameter list
        /// </returns>
        // ReSharper disable once MemberCanBePrivate.Global
        public bool Add(string parameterName, object value, string connectionName = "default")
        {
            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("The DataAccess class has not been initialized.");
            }

            if (FindParameter(parameterName) >= 0)
            {
                return false;
            }

            var conn = Environment.Connections[connectionName];

            //Using a switch statement in case new database types are added in the future
            switch (conn.DbServerType)
            {
                case DbServerType.postgresql:
                {
                    _parameters.Add(new NpgsqlParameter(parameterName, value));
                    break;
                }
                case DbServerType.mssql:
                {
                    _parameters.Add(new SqlParameter(parameterName, value));
                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException();
                }
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
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class</param>
        /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
        public bool Add<T>(string parameterName, IList<T> values, string typeName, string connectionName = "default")
        {
            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("The DataAccess class has not been initialized.");
            }

            if (values?.Any() != true)
            {
                return false;
            }

            var conn = Environment.Connections[connectionName];

            //Make sure the typeName contains a schema name. If it doesn't, prepend the default schema name.
            if (!typeName.StartsWith($"{conn.DefaultDbSchema}.") && !typeName.Contains('.'))
            {
                typeName = $"{conn.DefaultDbSchema}.{typeName}";
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
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class</param>
        /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
        public bool Add(string parameterName, DataTable table, string typeName = null,
            string connectionName = "default")
        {
            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("The DataAccess class has not been initialized.");
            }

            //if the table is null, or it does not contain rows...
            if (table?.Rows.Count == 0)
            {
                return false;
            }

            var conn = Environment.Connections[connectionName];

            //Switch is used to more easily add new database types in the future
            switch (conn.DbServerType)
            {
                case DbServerType.postgresql:
                {
                    //PostgreSql does not support using DataTables as parameters. It only supports json, so we need to convert the DataTable to json.
                    var data = table.Rows.Cast<DataRow>()
                        .Select(row => table.Columns.Cast<DataColumn>()
                            .ToDictionary(col => col.ColumnName, col => row[col]));

                    var json = JsonSerializer.Serialize(data);

                    var npgsqlParameter = new NpgsqlParameter(parameterName, json)
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    };

                    _parameters.Add(npgsqlParameter);
                    break;
                }
                case DbServerType.mssql:
                {
                    _parameters.Add(new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    });
                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException();
                }
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
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class.</param>
        /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
        public bool Add(string parameterName, DbType type, object value, string connectionName = "default")
        {
            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("The DataAccess class has not been initialized.");
            }

            if (FindParameter(parameterName) > 0)
            {
                return false;
            }

            var conn = Environment.Connections[connectionName];

            //Switch is used to more easily add new database types in the future
            switch (conn.DbServerType)
            {
                case DbServerType.postgresql:
                {
                    _parameters.Add(new NpgsqlParameter(parameterName, value)
                    {
                        DbType = type
                    });

                    break;
                }
                case DbServerType.mssql:
                {
                    _parameters.Add(new SqlParameter(parameterName, value)
                    {
                        DbType = type
                    });

                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException();
                }
            }

            return true;
        }

        /// <summary>
        /// Adds all properties from a <see cref="FlexObject"/> to the parameter collection.
        /// This is the recommended way to pass dynamic data with preserved type information.
        /// </summary>
        /// <param name="flexObject">The FlexObject instance.</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to "default".</param>
        /// <returns><c>true</c> if any parameters were added.</returns>
        public bool Add(FlexObject flexObject, string connectionName = "default")
        {
            ArgumentNullException.ThrowIfNull(flexObject);

            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("DataAccess must be initialized first.");
            }

            bool addedAny = false;

            foreach (var prop in flexObject)
            {
                if (!prop.Type.IsSqlConvertableType())
                {
                    throw new ArgumentException($"Property '{prop.Name}' has unsupported type '{prop.Type.Name}' for SQL parameter conversion.");
                }
                
                var paramName = "@" + prop.Name.LowercaseFirst();

                if (Add(paramName, prop.Value, connectionName))
                {
                    addedAny = true;
                }
            }

            return addedAny;
        }
        
        /// <summary>
        /// Adds all properties from a dynamic/ExpandoObject (or IDictionary&lt;string, object&gt;) 
        /// as individual parameters to the collection.
        /// </summary>
        /// <param name="dynamicObject">A dynamic object (ExpandoObject or IDictionary&lt;string, object&gt;).</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection.</param>
        /// <returns><c>true</c> if parameters were added successfully; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <b>Performance note (reflection overhead)</b>: This method uses reflection at runtime to discover
        /// properties. For high-throughput or performance-critical code, prefer strongly-typed POCOs
        /// with <see cref="Extensions.ToInsertParameters{T}"/> or <see cref="Extensions.ToUpdateParameters{T}"/>.
        /// </remarks>
        public bool AddFromDynamic(object dynamicObject, string connectionName = "default")
        {
            if (dynamicObject == null)
            {
                throw new ArgumentNullException(nameof(dynamicObject));
            }

            if (!Environment.IsInitialized)
            {
                throw new DataNotInitialized("DataAccess must be initialized first.");
            }

            IDictionary<string, object> dict;

            if (dynamicObject is IDictionary<string, object> d)
            {
                dict = d;
            }
            else if (dynamicObject is ExpandoObject expando)
            {
                dict = expando;
            }
            else
            {
                throw new ArgumentException(
                    "dynamicObject must be an ExpandoObject or IDictionary<string, object>.",
                    nameof(dynamicObject));
            }

            bool addedAny = false;

            foreach (var kvp in dict)
            {
                if (string.IsNullOrEmpty(kvp.Key))
                {
                    continue;
                }

                var paramName = "@" + kvp.Key.LowercaseFirst(); // uses the same convention as your extensions

                // Reuse existing Add logic (handles MSSQL vs PostgreSQL correctly)
                if (Add(paramName, kvp.Value, connectionName))
                    addedAny = true;
            }

            return addedAny;
        }

        /// <summary>
        /// Removes a parameter with the provided name from the parameter list.
        /// </summary>
        /// <param name="parameterName">Name of the parameter to remove.</param>
        /// <returns><c>true</c> if parameter was removed, <c>false</c> otherwise.</returns>
        public bool Remove(string parameterName)
        {
            var idx = FindParameter(parameterName);

            if (idx <= -1)
            {
                return false;
            }

            _parameters.RemoveAt(idx);

            return true;
        }

        /// <summary>
        /// Removes the parameter at the provided index.
        /// </summary>
        /// <param name="index">Index of the parameter to remove</param>
        public void RemoveAt(int index) => _parameters.RemoveAt(index);

        /// <summary>
        /// Clear all parameters from the parameter list.
        /// </summary>
        public void Clear() => _parameters.Clear();

        public IEnumerator<DbParameter> GetEnumerator() => _parameters.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose() => _parameters = null;

        /// <summary>
        /// Creates a new table valued parameter (primitive or SQL mappable type (eg. int, string, DateTime, Guid)) 
        ///  with the given name, <see cref="IEnumerable{T}"/> value, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <remarks>
        /// Make sure T is a SQL convertable type!!!
        /// Bad things will happen otherwise!
        /// </remarks>
        /// <typeparam name="T">Enumerable type</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="values">Enumerable values.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool AddSimpleTVP<T>(string parameterName, IEnumerable<T> values,
            string typeName, string connectionName = "default")
        {
            if (FindParameter(parameterName) >= 0)
            {
                return false;
            }

            var conn = Environment.Connections[connectionName];

            //Switch is used to more easily add new database types in the future
            switch (conn.DbServerType)
            {
                case DbServerType.postgresql:
                {
                    //PostgreSQL does not support table valued parameters. So, we pass it as JSON and process it in
                    //  a stored procedure or function.
                    var json = JsonSerializer.Serialize(values);

                    _parameters.Add(new NpgsqlParameter(parameterName, json)
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    });
                    break;
                }
                case DbServerType.mssql:
                {
                    var table = new DataTable
                    {
                        Columns =
                        {
                            { "value", typeof(string) }
                        }
                    };

                    foreach (var item in values)
                    {
                        table.Rows.Add(item?.ToString());
                    }

                    _parameters.Add(new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    });

                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException();
                }
            }

            return true;
        }

        /// <summary>
        /// Creates a new table valued parameter (containing a complex type that cannot be converted directly to SQL type)
        ///  with the given name, <see cref="IList{T}"/> values, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <remarks>
        /// Make sure T is a SQL convertable type!!!
        /// Bad things will happen otherwise!
        /// </remarks>
        /// <typeparam name="T">Type in the Enumerator</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="values">Enumerable values.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool AddComplexTVP<T>(
            string parameterName, IList<T> values,
            string typeName, string connectionName = "default")
        {
            if (FindParameter(parameterName) >= 0)
            {
                return false;
            }

            var t = typeof(T);

            if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(t) ||
                typeof(IDictionary<string, object>).IsAssignableFrom(t))
            {
                return AddDynamicTVP(parameterName, values.Cast<IDictionary<string, object>>().ToList(), typeName,
                    connectionName);
            }

            return AddComplexPocoTVP(parameterName, values, typeName, connectionName);
        }

        /// <summary>
        /// Handles dynamic/ExpandoObject TVPs securely for both databases.
        /// </summary>
        /// <remarks>
        /// Make sure T is a SQL convertable type!!!
        /// Bad things will happen otherwise!
        /// </remarks>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="values">Dictionary containing name/value pairs</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool AddDynamicTVP(string parameterName, IList<IDictionary<string, object>> values,
            string typeName, string connectionName = "default")
        {
            if (values == null || values.Count == 0)
            {
                return false;
            }

            // Security: prevent ridiculously wide TVPs
            if (values[0].Count > 64)
            {
                throw new ArgumentException("Dynamic TVP column count exceeds safe limit (64).", nameof(values));
            }

            var conn = Environment.Connections[connectionName];

            switch (conn.DbServerType)
            {
                case DbServerType.postgresql:
                {
                    var json = JsonSerializer.Serialize(values); // perfect for dynamic data

                    _parameters.Add(new NpgsqlParameter(parameterName, json)
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    });

                    break;
                }
                case DbServerType.mssql:
                {
                    var table = new DataTable();

                    // Use first item to define columns (all subsequent items must match)
                    var firstItem = values[0];

                    foreach (var kvp in firstItem)
                    {
                        var colType = kvp.Value?.GetType() ?? typeof(object);

                        table.Columns.Add(kvp.Key, ProcessColumnType(colType));
                    }

                    foreach (var dict in values)
                    {
                        // Security: ensure consistent shape
                        if (dict.Count != table.Columns.Count)
                        {
                            throw new ArgumentException("All dynamic TVP items must have the same column set.",
                                nameof(values));
                        }

                        var row = table.NewRow();

                        foreach (var kvp in dict)
                        {
                            row[kvp.Key] = kvp.Value ?? DBNull.Value;
                        }

                        table.Rows.Add(row);
                    }

                    _parameters.Add(new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    });

                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException();
                }
            }

            return true;
        }

        /// <summary>
        /// Creates a new table valued parameter (containing a complex type that cannot be converted directly to SQL type)
        ///  with the given name, <see cref="IList{T}"/> values, and SQL table valued type
        ///  then adds it to the list of parameters.
        /// </summary>
        /// <typeparam name="T">Type in the Enumerator</typeparam>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="values">List of values.</param>
        /// <param name="typeName">SQL Table Valued Parameter Type</param>
        /// <param name="connectionName">Name of connection to utilize. Defaults to the "default" connection, specified during Initialization of the DataAccess class.</param>
        /// <returns><c>true</c> if table valued parameter was created and added, <c>false</c> otherwise.</returns>
        // ReSharper disable once InconsistentNaming
        private bool AddComplexPocoTVP<T>(string parameterName, IList<T> values, string typeName,
            string connectionName = "default")
        {
            if (FindParameter(parameterName) >= 0)
            {
                return false;
            }

            var conn = Environment.Connections[connectionName];

            switch (conn.DbServerType)
            {
                case DbServerType.postgresql:
                {
                    var json = EntityListToJson<T>(values);

                    _parameters.Add(new NpgsqlParameter(parameterName, json)
                    {
                        NpgsqlDbType = NpgsqlDbType.Jsonb
                    });

                    break;
                }
                case DbServerType.mssql:
                {
                    var table = new DataTable();

                    var properties = typeof(T).GetProperties()
                        .Where(p => p.GetCustomAttribute<TVPIgnore>() == null)
                        .ToList();

                    foreach (var p in properties)
                    {
                        var attr = p.GetCustomAttribute<TVPColumn>();
                        var colName = attr?.ColumnName ?? p.Name;

                        table.Columns.Add(colName, ProcessColumnType(p.PropertyType));
                    }

                    foreach (var item in values)
                    {
                        var row = table.NewRow();

                        foreach (var p in properties)
                        {
                            var attr = p.GetCustomAttribute<TVPColumn>();
                            var colName = attr?.ColumnName ?? p.Name;

                            row[colName] = p.GetValue(item) ?? DBNull.Value;
                        }

                        table.Rows.Add(row);
                    }

                    _parameters.Add(new SqlParameter(parameterName, table)
                    {
                        SqlDbType = SqlDbType.Structured,
                        TypeName = typeName
                    });

                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException();
                }
            }

            return true;
        }

        /// <summary>
        /// Converts a list of entities to JSON.
        /// </summary>
        /// <param name="value">The List of entities.</param>
        /// <typeparam name="T">Type of entities.</typeparam>
        /// <returns>JSON output.</returns>
        private static string EntityListToJson<T>(IList<T> value)
        {
            if (value == null || value.Count == 0)
            {
                return "[]";
            }

            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<TVPIgnore>() == null)
                .ToList();

            var listOfDicts = value.Select(item =>
            {
                var dict = new Dictionary<string, object?>();
                foreach (var p in properties)
                {
                    var attr = p.GetCustomAttribute<TVPColumn>();
                    var colName = attr?.ColumnName ?? p.Name;
                    dict[colName] = p.GetValue(item);
                }

                return dict;
            }).ToList();

            return JsonSerializer.Serialize(listOfDicts);
        }

        /// <summary>
        /// Retrieves a type that is compatable with a <see cref="DataColumn"/>.
        /// </summary>
        /// <remarks>Basically, checks for <see cref="Nullable"/> and gets the underlying type if it is.</remarks>
        /// <param name="type">The type.</param>
        /// <returns>Compatable type.</returns>
        private static Type ProcessColumnType(Type type) => Nullable.GetUnderlyingType(type) ?? type;
    }
}