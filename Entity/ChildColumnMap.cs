using System;

namespace Database.Entity
{
  /// <summary>
  /// Provides child object property/column mapping from the parent datatable. However, child attributes
  /// (such as ColumnName) are ignored when creating the child item.
  /// </summary>
  [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
  public class ChildColumnMap : ColumnName
  {
    public string ChildPropertyName { get; private set; }

    public ChildColumnMap(string columnName, string childPropertyName)
      : base(columnName)
    {
      ChildPropertyName = childPropertyName;
    }
  }
}
