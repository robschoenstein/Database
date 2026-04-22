namespace Database.Attributes
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