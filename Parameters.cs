using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Database.Entity;

namespace Database
{
  public class Parameters : IEnumerable<SqlParameter>, IDisposable
  {
    private List<SqlParameter> _parameters = new List<SqlParameter>();

    public SqlParameter this[int index]
    {
      get { return _parameters[index]; }
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
    { }

    public Parameters(string parameterName, object value)
    {
      Add(parameterName, value);
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
    public static string BuildXmlString <T>(string xmlRootName, string item, List<T> values)
    {
      if (!typeof (T).IsSqlConvertableType())
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
    public bool Add(SqlParameter parameter)
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
    /// <returns>
    /// True if the parameter was added. False if a parameter with the same name already
    ///             existed in the parameter list
    /// </returns>
    public bool Add(string parameterName, object value)
    {
      if (FindParameter(parameterName) > 0)
      {
        return false;
      }

      _parameters.Add(new SqlParameter(parameterName, value));

      return true;
    }

    /// <summary>
    /// Adds a table valued parameter of the specified type
    /// </summary>
    /// <typeparam name="T">Type contained in collection</typeparam>
    /// <param name="parameterName">Name of the parameter.</param>
    /// <param name="values">Collection of values.</param>
    /// <param name="typeName">Table valued parameter name (eg. dbo.SimpleValueType).</param>
    /// <returns><c>true</c> if parameter was added, <c>false</c> otherwise.</returns>
    public bool Add <T>(string parameterName, IList<T> values, string typeName)
    {
      if (!values.Any())
      {
        return false;
      }

      if (!typeName.StartsWith("dbo."))
      {
        typeName = string.Format("dbo.{0}", typeName);
      }

      return typeof (T).IsSqlConvertableType()
        ? AddSimpleTVP(parameterName, values, typeName)
        : AddComplexTVP(parameterName, values, typeName);
    }

    public bool Add(string parameterName, DataTable table, string typeName)
    {
      var sqlParameter = new SqlParameter(parameterName, table)
        {
          SqlDbType = SqlDbType.Structured,
          TypeName = typeName
        };

      _parameters.Add(sqlParameter);
      return true;
    }

    /// <summary>
    /// Creates a new parameter with the given name, database type, and value 
    /// then adds it to the list of parameters.
    /// </summary>
    /// <param name="parameterName">Name of the parameter</param>
    /// <param name="type">Type of the parameter</param>
    /// <param name="value">Value of the parameter</param>
    /// <returns>
    /// True if the parameter was added. False if a parameter with the same name already
    ///             existed in the parameter list
    /// </returns>
    public bool Add(string parameterName, SqlDbType type, object value)
    {
      if (FindParameter(parameterName) > 0)
      {
        return false;
      }

      var sqlParameter = new SqlParameter(parameterName, value)
        {
          SqlDbType = type
        };

      _parameters.Add(sqlParameter);

      return true;
    }

    /// <summary>
    /// Removes a parameter with the provided name from the parameter list.
    /// </summary>
    /// <param name="parameterName">Name of the parameter to remove.</param>
    /// <returns>
    /// True if the parameter was found and removed. Otherwise, returns false.
    /// </returns>
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

    public IEnumerator<SqlParameter> GetEnumerator()
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
    ///  with the given name, <see cref="Enumerable{T}"/> value, and SQL table valued type
    ///  then adds it to the list of parameters.
    /// </summary>
    /// <typeparam name="T">Enumerable type</typeparam>
    /// <param name="parameterName">Parameter name.</param>
    /// <param name="value">Enumerable value.</param>
    /// <param name="typeName">SQL Table Valued Parameter Type</param>
    /// <param name="sqlColLength">If property is a string, the sql column length is required</param>
    /// <param name="sqlDataType">The SQL data type to use.</param>
    /// <returns><c>true</c> if item has been added, <c>false</c> otherwise.</returns>
    private bool AddSimpleTVP <T>(
      string parameterName, IEnumerable<T> value, string typeName)
    {
      if (FindParameter(parameterName) > 0)
      {
        return false;
      }

      if (!typeof (T).IsSqlConvertableType())
      {
        throw new ArgumentException(
          "value parameter must be an IEnumerable<T> of a primitive or SQL mappable type (eg. int, string, DateTime, Guid).",
          "value");
      }

      var table = new DataTable();

      table.Columns.Add("value", typeof (string));

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
      return true;
    }

    /// <summary>
    /// Creates a new table valued parameter (containing a complex type that cannot be converted directly to SQL type)
    ///  with the given name, <see cref="Enumerable{T}"/> value, and SQL table valued type
    ///  then adds it to the list of parameters.
    /// </summary>
    /// <typeparam name="T">Enumerable type</typeparam>
    /// <param name="parameterName">Parameter name.</param>
    /// <param name="value">Enumerable value.</param>
    /// <param name="typeName">SQL Table Valued Parameter Type</param>
    /// <param name="restrictWithTVPColumnAttribute">Restrict property retrieval using <see cref="TVPColumn"/> attribute</param>
    /// <returns><c>true</c> if item has been added, <c>false</c> otherwise.</returns>
    private bool AddComplexTVP <T>(
      string parameterName, IList<T> value,
      string typeName)
    {
      if (FindParameter(parameterName) > 0)
      {
        return false;
      }

      if (typeof (T).IsSqlConvertableType())
      {
        throw new ArgumentException(
          "value parameter must be an IEnumerable<T> of a complex type (eg. object consisting of multiple properties).",
          "value");
      }


      if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(typeof(T)))
      {
        throw new ArgumentException(
          "value parameter cannot contain dynamic objects... yet.",
          "value");
      }

      return CreateComplexTVP(parameterName, value, typeName);
    }

    private bool CreateComplexTVP <T>(string parameterName, IEnumerable<T> value, string typeName)
    {
      var properties = typeof (T).GetProperties()
        .Where(p => p.GetCustomAttributes(typeof (TVPIgnore), false).Length < 1)
        .ToList();

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
          var attr = (TVPColumn[])properties[i].GetCustomAttributes(typeof (TVPColumn), false);

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

      return true;
    }

    private bool CreateComplexTVPFromExpando <T>(string parameterName, IList<T> value,
      string typeName)
    {
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

      return true;
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