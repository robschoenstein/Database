using System;

namespace Database.Entity
{
  [AttributeUsage(AttributeTargets.Property)]
  public class ColumnName : Attribute
  {
    public string Name
    {
      get;
      private set;
    }

    public ColumnName(string columnName)
    {
      Name = columnName;
    }
  }
}
