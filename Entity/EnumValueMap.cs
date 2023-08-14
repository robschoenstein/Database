using System;

namespace Database.Entity
{
  /// <summary>
  /// Used to map database values to local enum values
  /// </summary>
  [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
  public class EnumValueMap : Attribute
  {
    public string EnumValue { get; private set; }
    public string DatabaseValue { get; set; }

    public EnumValueMap(string enumValue, string databaseValue)
    {
      EnumValue = enumValue;
      DatabaseValue = databaseValue;
    }
  }
}