using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Database.Entity;

namespace Database
{
  //TODO: Add support for ChildColumnMap
  internal class ObjectCache
  {
    private static ObjectCache _instance;

    public static ObjectCache Instance
    {
      get { return _instance ?? (_instance = new ObjectCache()); }
    }

    public ConcurrentDictionary<Type, ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>> Cache { get; set; }

    private ObjectCache()
    {
      Cache = new ConcurrentDictionary<Type, ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>>();
    }

    public bool ContainsProperty(Type type, string propertyName)
    {
      return Cache.ContainsKey(type) && Cache[type].Any(o => o.Key.Name == propertyName);
    }

    public bool ContainsColumnNameAttribute(Type type, string columnName)
    {
      return Cache.ContainsKey(type) && Cache[type].Any(o => o.Value.Any(a => ((ColumnName)a).Name.ToUpper() == columnName.ToUpper()));
    }

    public bool ContainsChildColumnMapAttribute(Type type)
    {
      return Cache.ContainsKey(type) && Cache[type].Any(o => o.Value.Any(a => a is ChildColumnMap));
    }

    public bool ContainsEnumValueMap(PropertyInfo propertyInfo)
    {
      return Cache.ContainsKey(propertyInfo.ReflectedType) && Cache[propertyInfo.ReflectedType]
        .Any(o => o.Key.Name == propertyInfo.Name &&
        o.Value.Any(a => a is EnumValueMap));
    }

    public void AddCacheObject(Type type, PropertyInfo property, Attribute[] attributes)
    {
      if (!Cache.ContainsKey(type))
        Cache.TryAdd(type, new ConcurrentBag<KeyValuePair<PropertyInfo, Attribute[]>>());

      if (!ContainsProperty(type, property.Name))
        Cache[type].Add(new KeyValuePair<PropertyInfo, Attribute[]>(property, attributes));
    }

    public List<PropertyInfo> RetrieveMatchingProperties(string name, Type type)
    {
      return (Cache[type].Where(kvp => kvp.Key.Name == name ||
                                       kvp.Value.Any(a => ((ColumnName)a).Name == name))
        .Select(kvp => kvp.Key)).ToList();
    }

    public EnumValueMap[] RetrieveEnumValueMap(PropertyInfo propertyInfo)
    {
      return (EnumValueMap[])Cache[propertyInfo.ReflectedType].FirstOrDefault(kvp => kvp.Key.Name == propertyInfo.Name &&
                                       kvp.Value.Any(a => a is EnumValueMap)).Value;
    }
  }
}
