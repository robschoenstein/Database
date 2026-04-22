using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Database.Attributes;

namespace Database.Cache
{
    internal sealed class ObjectCache
    {
        private static ObjectCache _instance = new();

        public static ObjectCache Instance => _instance;

        public ConcurrentDictionary<Type, ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>> Cache
        {
            get;
            private set;
        }

        private ObjectCache()
        {
            Cache = new ConcurrentDictionary<Type, ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>>();
        }

        /// <summary>Ensures a type’s properties and attributes are cached.</summary>
        internal void EnsureTypeCached(Type type)
        {
            if (Cache.ContainsKey(type))
            {
                return;
            }

            var bag = new ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>();
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            foreach (var property in properties)
            {
                var attributes = Attribute.GetCustomAttributes(property).ToArray();
                bag.Add(new KeyValuePair<PropertyInfo, Attribute[]>(property, attributes));
            }

            Cache.TryAdd(type, bag);
        }

        internal bool ContainsProperty(Type type, string propertyName)
        {
            return Cache.ContainsKey(type) &&
                   Cache[type].Any(o =>
                       o.Key.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase));
        }

        internal bool ContainsColumnNameAttribute(Type type, string columnName)
        {
            return Cache.ContainsKey(type) &&
                   Cache[type].Any(o => o.Value.OfType<ColumnName>()
                       .Any(a => a.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase)));
        }

        internal bool ContainsChildColumnMapAttribute(Type type)
        {
            return Cache.ContainsKey(type) &&
                   Cache[type].Any(o => o.Value.OfType<ChildColumnMap>().Any());
        }

        internal bool ContainsEnumValueMap(PropertyInfo propertyInfo)
        {
            if (propertyInfo.ReflectedType == null || !Cache.ContainsKey(propertyInfo.ReflectedType))
            {
                return false;
            }

            return Cache[propertyInfo.ReflectedType]
                .Any(o => o.Key.Name == propertyInfo.Name && o.Value.OfType<EnumValueMap>().Any());
        }

        internal bool HasInsertParamIgnore(PropertyInfo property)
        {
            return GetAttributes(property).OfType<InsertParamIgnore>().Any();
        }

        internal bool HasUpdateParamIgnore(PropertyInfo property)
        {
            return GetAttributes(property).OfType<UpdateParamIgnore>().Any();
        }

        internal void AddCacheObject(Type type, PropertyInfo property, Attribute[] attributes)
        {
            if (!Cache.ContainsKey(type))
            {
                Cache.TryAdd(type, new ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>());
            }

            if (!ContainsProperty(type, property.Name))
            {
                Cache[type].Add(new KeyValuePair<PropertyInfo, Attribute[]>(property, attributes));
            }
        }

        internal List<PropertyInfo> RetrieveMatchingProperties(string name, Type type)
        {
            EnsureTypeCached(type);

            return Cache[type]
                .Where(kvp => kvp.Key.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                              kvp.Value.OfType<ColumnName>()
                                  .Any(a => a.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                .Select(kvp => kvp.Key)
                .ToList();
        }

        internal EnumValueMap[] RetrieveEnumValueMap(PropertyInfo propertyInfo)
        {
            EnsureTypeCached(propertyInfo.ReflectedType!);

            var kvp = Cache[propertyInfo.ReflectedType!]
                .FirstOrDefault(o => o.Key.Name == propertyInfo.Name &&
                                     o.Value.OfType<EnumValueMap>().Any());

            return kvp.Value?.OfType<EnumValueMap>().ToArray() ?? Array.Empty<EnumValueMap>();
        }
        
        internal List<(PropertyInfo ParentProperty, ChildColumnMap Map)> GetChildColumnMaps(Type type, string columnName)
        {
            EnsureTypeCached(type);

            var matches = new List<(PropertyInfo, ChildColumnMap)>();

            foreach (var kvp in Cache[type])
            {
                var childMaps = kvp.Value
                    .OfType<ChildColumnMap>()
                    .Where(m => m.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                foreach (var map in childMaps)
                    matches.Add((kvp.Key, map));
            }

            return matches;
        }
        
        private IEnumerable<Attribute> GetAttributes(PropertyInfo property)
        {
            var type = property.ReflectedType ?? property.DeclaringType;
            if (type != null && Cache.TryGetValue(type, out var bag))
            {
                var entry = bag.FirstOrDefault(kvp => kvp.Key.Name == property.Name);
                if (entry.Value != null)
                    return entry.Value;
            }
            return Attribute.GetCustomAttributes(property);
        }
    }
}