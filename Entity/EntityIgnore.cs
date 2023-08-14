using System;

namespace Database.Entity
{
  /// <summary>
  /// Ignores property when populating entities with data
  /// </summary>
  /// <remarks>
  /// Useful when database returns a column name that matches one property, but needs to populate another.
  /// </remarks>
  [AttributeUsage(AttributeTargets.Property)]
  public class EntityIgnore : Attribute
  {
  }
}
