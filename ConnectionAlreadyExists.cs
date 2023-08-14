using System;

namespace Database
{
  /// <summary>
  /// Exception indicating that an attempt has been made to add a connection
  ///             that already exists.
  /// 
  /// </summary>
  public class ConnectionAlreadyExists : ApplicationException
  {
    public ConnectionAlreadyExists(string message)
      : base(message)
    {
    }
  }
}
