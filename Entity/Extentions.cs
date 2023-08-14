using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Database.Entity
{
  public static class Extentions
  {
    /// <summary>
    /// Create an entity from a <see cref="DataRow"/>.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <param name="dataRow">The <see cref="DataRow"/>.</param>
    /// <returns>Populated entity.</returns>
    public static T ToEntity<T>(this DataRow dataRow)
    {
      var entity = default(T);

      try
      {
        entity = Activator.CreateInstance<T>();
      }
      catch (Exception ex)
      {
        throw new Exception(string.Format("Default (parameterless) constructor does not exist for type: {0}", typeof(T).Name), ex);
      }

      PopulateEntity.ToEntity(entity, dataRow);

      return entity;
    }

    /// <summary>
    /// Populates an existing entity from a <see cref="DataRow"/>.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="dataRow">The <see cref="DataRow"/>.</param>
    /// <param name="entity">The entity.</param>
    public static void ToEntity<T>(this DataRow dataRow, T entity)
    {
      PopulateEntity.ToEntity(entity, dataRow);
    }

    /// <summary>
    /// Create an entity from first <see cref="DataRow"/> contained within the <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    public static T ToEntity<T>(this DataTable dataTable)
    {
      var entity = default(T);

      if (dataTable.Rows.Count > 0)
      {
        try
        {
          entity = Activator.CreateInstance<T>();
        }
        catch (Exception ex)
        {
          throw new Exception(string.Format("Default (parameterless) constructor does not exist for type: {0}", typeof(T).Name), ex);
        }

        PopulateEntity.ToEntity(entity, dataTable.Rows[0]);
      }

      return entity;
    }

    /// <summary>
    /// Populates an existing entity from first <see cref="DataRow"/> contained within the <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    /// <param name="entity">The entity.</param>
    public static void ToEntity<T>(this DataTable dataTable, T entity)
    {
      if (dataTable.Rows.Count > 0)
        PopulateEntity.ToEntity(entity, dataTable.Rows[0]);
    }

    /// <summary>
    /// Create an entity from specified <see cref="DataRow"/> contained within the <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    /// <param name="rowIndex">Index of the <see cref="DataRow"/>.</param>
    public static T ToEntity<T>(this DataTable dataTable, int rowIndex)
    {
      var entity = default(T);

      if (dataTable.Rows.Count > rowIndex)
      {
        try
        {
          entity = Activator.CreateInstance<T>();
        }
        catch (Exception ex)
        {
          throw new Exception(string.Format("Default (parameterless) constructor does not exist for type: {0}", typeof(T).Name), ex);
        }

        PopulateEntity.ToEntity(entity, dataTable.Rows[rowIndex]);
      }

      return entity;
    }

    /// <summary>
    /// Populates an existing entity from specified <see cref="DataRow"/> contained within the <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    /// <param name="entity">The entity.</param>
    /// <param name="rowIndex">Index of the <see cref="DataRow"/>.</param>
    public static void ToEntity<T>(this DataTable dataTable, T entity, int rowIndex)
    {
      if (dataTable.Rows.Count > rowIndex)
        PopulateEntity.ToEntity(entity, dataTable.Rows[rowIndex]);
    }

    /// <summary>
    /// Creates and populates entities for each <see cref="DataRow"/> contained within the <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    /// <returns><see cref="IEnumerable"/> containing the entities.</returns>
    public static IEnumerable<T> ToEntities<T>(this DataTable dataTable)
    {
      return PopulateEntity.ToEntities<T>(dataTable);
    }

    /// <summary>
    /// Creates and populates entities for each <see cref="DataRow"/> contained within the first <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity Type</typeparam>
    /// <param name="dataSet">The <see cref="DataSet"/>.</param>
    /// <returns><see cref="IEnumerable"/> containing the entities.</returns>
    public static IEnumerable<T> ToEntities<T>(this DataSet dataSet)
    {
      return PopulateEntity.ToEntities<T>(dataSet.Tables[0]);
    }

    /// <summary>
    /// Creates and populates entities for each <see cref="DataRow"/> contained within the specified <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">Entity Type</typeparam>
    /// <param name="dataSet">The <see cref="DataSet"/>.</param>
    /// <param name="tableIndex">Index of the table.</param>
    /// <returns><see cref="IEnumerable"/> containing the entities.</returns>
    public static IEnumerable<T> ToEntities<T>(this DataSet dataSet, int tableIndex)
    {
      return PopulateEntity.ToEntities<T>(dataSet.Tables[tableIndex]);
    }

    public static bool IsSqlConvertableType(this Type type)
    {
      return type.IsValueType ||
             type.IsPrimitive ||
             new Type[]
               {
                 typeof (String),
                 typeof (Decimal),
                 typeof (DateTime),
                 typeof (DateTimeOffset),
                 typeof (TimeSpan),
                 typeof (Guid)
               }.Any(t => t == type)
             || Convert.GetTypeCode(type) != TypeCode.Object;
    }
  }
}
