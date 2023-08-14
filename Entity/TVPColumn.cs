using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Database.Entity
{
  [AttributeUsage(AttributeTargets.Property)]
  public class TVPColumn : Attribute
  {
    public string ColumnName { get; private set; }

    public TVPColumn()
    { }

    public TVPColumn(string columnName)
    {
      ColumnName = columnName;
    }
  }
}