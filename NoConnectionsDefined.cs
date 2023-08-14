using System;

namespace Database
{
  /// <summary>
  /// Exception indicating that the no connections have been defined.
  /// 
  /// </summary>
  public class NoConnectionsDefined : ApplicationException
  {
    public NoConnectionsDefined(string message)
      : base(message)
    {
    }
  }
}
