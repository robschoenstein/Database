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
using Database.Attributes;
using Database.Cache;
using Database.Exceptions;

namespace Database.Entity
{
    internal static class PopulateEntity
    {
        /// <summary>
        /// Reads a <see cref="DataRow"/> into an existing entity.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="entity">Entity to be populated.</param>
        /// <param name="dataRow">The <see cref="DataRow"/>.</param>
        public static void ToEntity<T>(T entity, DataRow dataRow)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(dataRow);
            
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
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(dataTable);
            
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
            ArgumentNullException.ThrowIfNull(dataTable);
            
            var columns = dataTable.Columns.Cast<DataColumn>().ToList();
            var dataList = new ConcurrentDictionary<int, T>();

            Parallel.For(0, dataTable.Rows.Count, i =>
            {
                var entity = Activator.CreateInstance<T>();
                Populate(dataTable.Rows[i], entity, columns);
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
            var entityType = entity!.GetType();
            var cache = ObjectCache.Instance;
            cache.EnsureTypeCached(entityType);

            for (var i = 0; i < columns.Count; i++)
            {
                var columnName = columns[i].ColumnName;
                var columnValue = dataRow[i];

                var matchingProperties = RetrieveMatchingProperties(columnName, entityType);

                foreach (var pi in matchingProperties)
                {
                    try
                    {
                        // ChildColumnMap support (fully restored and improved)
                        var childMaps = cache.GetChildColumnMaps(entityType, columnName);
                        var childMap = childMaps.FirstOrDefault(m => m.ParentProperty == pi).Map;

                        if (childMap != null)
                        {
                            HandleChildColumnMap(entity, pi, childMap, columnValue);
                            continue;
                        }

                        // Normal property population
                        var val = columnValue is DBNull 
                            ? GetDefault(pi.PropertyType) 
                            : ChangeType(columnValue, pi);

                        pi.SetValue(entity, val);
                    }
                    catch (TypeConversionException tce)
                    {
                        throw new PopulationException(tce.PropertyName, tce.PropertyType, columnName,
                            columnValue?.GetType(), columnValue,
                            $"Property Population Failed! Property: {tce.PropertyName}, Column: {columnName}", tce);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"PopulateEntity failed while setting '{pi.Name}' from column '{columnName}'", ex);
                    }
                }
            }
        }
        
        /// <summary>
        /// Handles [ChildColumnMap] – maps a flat column into a nested child object's property.
        /// Child object is auto-created if null.
        /// </summary>
        private static void HandleChildColumnMap<T>(T entity, PropertyInfo parentProperty, 
            ChildColumnMap map, object columnValue)
        {
            object? child = parentProperty.GetValue(entity);
            if (child == null)
            {
                child = Activator.CreateInstance(parentProperty.PropertyType);
                parentProperty.SetValue(entity, child);
            }

            var childProp = parentProperty.PropertyType.GetProperty(
                map.ChildPropertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

            if (childProp == null) return;

            var val = columnValue is DBNull 
                ? GetDefault(childProp.PropertyType) 
                : ChangeType(columnValue, childProp);

            childProp.SetValue(child, val);
        }

        // ReSharper disable once UnusedMember.Local
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
            return ObjectCache.Instance.RetrieveMatchingProperties(columnName, entityType);
        }

        private static object GetDefault(Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null!;
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
            ArgumentNullException.ThrowIfNull(propertyInfo);

            try
            {
                var conversionType = propertyInfo.PropertyType;

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
                    if (value == null || value == DBNull.Value)
                    {
                        return null!;
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
                    if (value is int i)
                    {
                        return Enum.ToObject(propertyInfo.PropertyType, i);
                    }

                    return GetEnumValue(propertyInfo, value?.ToString());
                }

                return Convert.ChangeType(value, conversionType);
            }
            catch (Exception ex)
            {
                throw new TypeConversionException(propertyInfo, "Type Conversion Failed", ex);
            }
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
            var cache = ObjectCache.Instance;
            var enumMaps = cache.ContainsEnumValueMap(propertyInfo)
                ? cache.RetrieveEnumValueMap(propertyInfo)
                : (EnumValueMap[])Attribute.GetCustomAttributes(propertyInfo, typeof(EnumValueMap));
            var map = enumMaps.FirstOrDefault(m => m.DatabaseValue == value);

            try
            {
                return map != null
                    ? Enum.Parse(propertyInfo.PropertyType, map.EnumValue)
                    : Enum.Parse(propertyInfo.PropertyType, value);
            }
            catch (Exception ex)
            {
                // Fallback to StringValueAttribute if present
                foreach (var val in Enum.GetValues(propertyInfo.PropertyType))
                {
                    var fi = propertyInfo.PropertyType.GetField(val.ToString()!);
                    var attr = fi?.GetCustomAttribute<StringValueAttribute>();
                    if (attr?.StringValue == value)
                        return val;
                }

                throw new ArgumentException($"Value '{value}' is not valid for enum {propertyInfo.PropertyType.Name}");
            }
        }
    }
}