using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Database.Entity
{
  internal class PopulateEntity
  {
    /// <summary>
    /// Reads a <see cref="DataRow"/> into an existing entity.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <param name="entity">Entity to be populated.</param>
    /// <param name="dataRow">The <see cref="DataRow"/>.</param>
    public static void ToEntity<T>(T entity, DataRow dataRow)
    {
      var columns = dataRow.Table.Columns.Cast<DataColumn>().ToList();

      //iterate through columns and set properties of the entity with the matching column values
      Populate(dataRow, entity, columns);
    }

    /// <summary>
    /// Reads specified <see cref="DataRow"/> into an existing entity.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <param name="entity">Entity to be populated.</param>
    /// <param name="dataTable">The <see cref="DataTable"/> containing specified row.</param>
    /// <param name="rowIndex">Index of the row.</param>
    public static void ToEntity<T>(T entity, DataTable dataTable, int rowIndex)
    {
      var columns = dataTable.Columns.Cast<DataColumn>().ToList();

      //iterate through columns and set properties of the entity with the matching column values
      Populate(dataTable.Rows[rowIndex], entity, columns);
    }

    /// <summary>
    /// Reads <see cref="DataTable"/> into an <see cref="IEnumerable"/> of the specified entity type.
    /// </summary>
    /// <typeparam name="T">The type of entity being used.</typeparam>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    /// <returns><see cref="IEnumerable"/> containing the entities.</returns>
    public static IEnumerable<T> ToEntities<T>(DataTable dataTable)
    {
      var columns = dataTable.Columns.Cast<DataColumn>().ToList();

      var cancelTokenSource = new CancellationTokenSource();

      //TODO: Create timer that cancels the parallel operation upon timeout.
      var options = new ParallelOptions
      {
        CancellationToken = cancelTokenSource.Token,
        MaxDegreeOfParallelism = 2
      };

      var dataList = new ConcurrentDictionary<int, T>();

      Parallel.For(0, dataTable.Rows.Count, options, (i, loopState) =>
        {
          if (loopState.ShouldExitCurrentIteration)
          {
            return;
          }

          var dataRow = dataTable.Rows[i];
          var entity = default(T);

          try
          {
            entity = Activator.CreateInstance<T>();
          }
          catch (Exception ex)
          {
            throw new Exception(string.Format("Default (parameterless) constructor does not exist for type: {0}", typeof(T).Name), ex);
          }

          //iterate through columns and set properties of the entity with the matching column values
          Populate(dataRow, entity, columns);

          //entity populated. Add it to the collection.
          dataList.TryAdd(i, entity);
        });

      return dataList.OrderBy(d => d.Key).Select(d => d.Value);
    }

    /// <summary>
    /// Populates the entity using the specified <see cref="DataRow"/>.
    /// </summary>
    /// <typeparam name="T">The type of entity being used.</typeparam>
    /// <param name="dataRow">The <see cref="DataRow"/>.</param>
    /// <param name="entity">The entity.</param>
    /// <param name="columns">Column list.</param>
    private static void Populate<T>(DataRow dataRow, T entity, IList<DataColumn> columns)
    {
      for (var i = 0; i < columns.Count(); i++)
      {
        try
        {
          var entityType = entity.GetType();

          var properties = RetrieveMatchingProperties(columns[i].ColumnName, entityType);

          if (properties.Count <= 0)
            continue;

          foreach (var pi in properties)
          {
            //TODO: need to cache the ChildColumnMap attributes
            //var childColumnMap = (ChildColumnMap[])Attribute.GetCustomAttributes(pi, typeof(ChildColumnMap));
            //if (childColumnMap == null || childColumnMap.Length <= 0)
            //{
            var val = dataRow[i] is DBNull ? GetDefault(pi.PropertyType) : ChangeType(dataRow[i], pi);
            pi.SetValue(entity, val, null);
            //}
            //else
            //{
            //  var val = pi.GetValue(entity, null);

            //  if (val == null)
            //    pi.SetValue(entity, Activator.CreateInstance(pi.PropertyType), null);
            //  val = pi.GetValue(entity, null);
            //  var attribute = childColumnMap.SingleOrDefault(m => m.Name.ToUpper().Equals(columns[i].ColumnName.ToUpper()));
            //  var cpi = GetChildPropertyInfo(val, attribute.ChildPropertyName);

            //  if (cpi == null)
            //    continue;

            //  cpi.SetValue(val, dataRow[i].GetType() == typeof(DBNull) ? GetDefault(cpi.PropertyType) : ChangeType(dataRow[i], cpi), null);
            //}
          }
        }
        catch (TypeConversionException tce)
        {
          throw new PopulationException(tce.PropertyName, tce.PropertyType, columns[i].ColumnName,
            dataRow[i].GetType(), dataRow[i],
            string.Format("Property Population Failed! Property Name: {0}, Property Type: {1}, Column Name: {2}, Column Type: {3}",
              tce.PropertyName, tce.PropertyType.Name, columns[i].ColumnName, dataRow[i].GetType().Name), tce);
        }
        catch (Exception ex)
        {
          throw new Exception("PopulateModel.cs --> PopulateEntity<T> : Error while populating entity", ex);
        }
      }
    }

    private static PropertyInfo GetChildPropertyInfo<T>(T entity, string propertyName)
    {
      var entityType = entity.GetType();
      var properties = entityType.GetProperties();

      var pi = (from p in properties
                where p.Name.ToUpper().Equals(propertyName.ToUpper())
                select p).SingleOrDefault();

      return pi;
    }

    private static List<PropertyInfo> RetrieveMatchingProperties(string columnName, Type entityType)
    {
      var objectCache = ObjectCache.Instance;

      if (objectCache.ContainsProperty(entityType, columnName))
      {
        return objectCache.RetrieveMatchingProperties(columnName, entityType);
      }

      var matches = new List<PropertyInfo>();

      var pi = entityType.GetProperty(columnName,
                                      BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

      if (pi != null && Attribute.GetCustomAttribute(pi, typeof(EntityIgnore)) == null)
      {
        matches.Add(pi);

        objectCache.AddCacheObject(entityType, pi, Attribute.GetCustomAttributes(pi, typeof(ColumnName)));
      }

      var properties = entityType.GetProperties();

      foreach (var propertyInfo in properties.Where(p => pi == null || p != pi))
      {
        if (objectCache.ContainsColumnNameAttribute(entityType, propertyInfo.Name)
            || Attribute.GetCustomAttribute(propertyInfo, typeof(EntityIgnore)) != null)
          continue;

        var attributes = Attribute.GetCustomAttributes(propertyInfo, typeof(ColumnName)).Where(a => ((ColumnName)a).Name == columnName).ToArray();

        if (attributes.Length <= 0)
          continue;

        matches.Add(propertyInfo);
        objectCache.AddCacheObject(entityType, propertyInfo, attributes);
      }

      return matches;
    }

    private static object GetDefault(Type type)
    {
      return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>
    /// Returns an Object with the specified Type and whose value is equivalent to the specified object.
    /// </summary>
    /// <param name="value">An Object that implements the IConvertible interface.</param>
    /// <param name="propertyInfo"><see cref="PropertyInfo"/> of property to populate with supplied value.</param>
    /// <returns>An object whose Type is conversionType (or conversionType's underlying type if conversionType
    /// is Nullable&lt;&gt;) and whose value is equivalent to value. -or- a null reference, if value is a null
    /// reference and conversionType is not a value type.</returns>
    /// <remarks>
    /// This method exists as a workaround to System.Convert.ChangeType(Object, Type) which does not handle
    /// nullables as of version 2.0 (2.0.50727.42) of the .NET Framework. The idea is that this method will
    /// be deleted once Convert.ChangeType is updated in a future version of the .NET Framework to handle
    /// nullable types, so we want this to behave as closely to Convert.ChangeType as possible.
    /// This method was written by Peter Johnson at:
    /// http://aspalliance.com/author.aspx?uId=1026.
    /// </remarks>
    public static object ChangeType(object value, PropertyInfo propertyInfo)
    {
      object newValue = null;

      try
      {
        var conversionType = propertyInfo.PropertyType;

        // Note: This if block was taken from Convert.ChangeType as is, and is needed here since we're
        // checking properties on conversionType below.
        if (conversionType == null)
        {
          throw new ArgumentNullException("conversionType");
        } // end if

        // If it's not a nullable type, just pass through the parameters to Convert.ChangeType

        if (conversionType.IsGenericType &&
          conversionType.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
          // It's a nullable type, so instead of calling Convert.ChangeType directly which would throw a
          // InvalidCastException (per http://weblogs.asp.net/pjohnson/archive/2006/02/07/437631.aspx),
          // determine what the underlying type is
          // If it's null, it won't convert to the underlying type, but that's fine since nulls don't really
          // have a type--so just return null
          // Note: We only do this check if we're converting to a nullable type, since doing it outside
          // would diverge from Convert.ChangeType's behavior, which throws an InvalidCastException if
          // value is null and conversionType is a value type.
          if (value == null)
          {
            return null;
          } // end if

          // It's a nullable type, and not null, so that means it can be converted to its underlying type,
          // so overwrite the passed-in conversion type with this underlying type
          var nullableConverter = new NullableConverter(conversionType);
          conversionType = nullableConverter.UnderlyingType;
        } // end if

        // Now that we've guaranteed conversionType is something Convert.ChangeType can handle (i.e. not a
        // nullable type), pass the call on to Convert.ChangeType
        if (conversionType.IsEnum)
        {
          if (value is int)
            return System.Enum.ToObject(propertyInfo.PropertyType, value);

          return GetEnumValue(propertyInfo, (string)value);
        }

        newValue = Convert.ChangeType(value, conversionType);

      }
      catch (Exception ex)
      {
        throw new TypeConversionException(propertyInfo, "Type Conversion Failed", ex);
      }

      return newValue;
    }

    /// <summary>
    /// Creates an enumerated value type.
    /// </summary>
    /// <param name="propertyInfo"></param>
    /// <param name="value"></param>
    /// <returns> If the passed in value matches an element within the enumeration or within a defined
    /// map, the matching enumerated value is returned. Otherwise, the default enumerated value is returned.</returns>
    private static object GetEnumValue(PropertyInfo propertyInfo, string value)
    {
      var objectCache = ObjectCache.Instance;

      EnumValueMap[] enumValueMap = null;

      if (objectCache.ContainsEnumValueMap(propertyInfo))
        enumValueMap = objectCache.RetrieveEnumValueMap(propertyInfo);
      else
        enumValueMap = (EnumValueMap[])Attribute.GetCustomAttributes(propertyInfo, typeof(EnumValueMap));

      EnumValueMap map = null;

      if (enumValueMap.Length > 0)
      {
        map = enumValueMap.FirstOrDefault(m => m.DatabaseValue == value);
      }

      try
      {
        return map != null ? Enum.Parse(propertyInfo.PropertyType, map.EnumValue) :
           Enum.Parse(propertyInfo.PropertyType, value);
      }
      catch (Exception ex)
      {
        try
        {
          foreach (var val in from Enum val in Enum.GetValues(propertyInfo.PropertyType)
                              let fi = propertyInfo.PropertyType.GetField(val.ToString())
                              let attributes = (StringValueAttribute[])fi.GetCustomAttributes(
                                typeof(StringValueAttribute), false)
                              let attr = attributes[0]
                              where attr.StringValue == value
                              select val)
          {
            return Convert.ChangeType(val, propertyInfo.PropertyType);
          }
        }
        catch (Exception ex2)
        {
          throw new ArgumentException("The value '" + value + "' is not contained in " + propertyInfo.PropertyType.Name, ex2);
        }

        throw new ArgumentException("The value '" + value + "' is not contained in " + propertyInfo.PropertyType.Name, ex);
      }
    }
  }
}
