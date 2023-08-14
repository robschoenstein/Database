using System.Collections;

namespace Database
{
  public class ConnectionList
  {
    private string _defaultConnection = string.Empty;
    private Hashtable _connections = new Hashtable();

    public string DefaultConnection
    {
      get
      {
        return this._defaultConnection;
      }
      set
      {
        if (!this._connections.ContainsKey((object)value))
          throw new UnknownConnection("The default connection must already be defined within Connections. The connection '" + value + "' is unknown.");
        this._defaultConnection = value;
      }
    }

    public string this[string key]
    {
      get
      {
        if (this._connections.ContainsKey((object)key))
          return (string)this._connections[(object)key];
        else
          throw new UnknownConnection("Unable to retrieve connection. The provided connection '" + key + "' is not a known connection.");
      }
      set
      {
        if (!this._connections.ContainsKey((object)value))
          throw new UnknownConnection("Unable to set connection. The provided connection '" + key + "' is not a known connection.");
        this._connections[(object)key] = (object)value;
      }
    }

    /// <summary>
    /// Adds the key value pair to the Connection List
    /// 
    /// </summary>
    /// <param name="key"/><param name="value"/>
    public void Add(string key, string value)
    {
      if (this._connections.ContainsKey((object)key))
        throw new ConnectionAlreadyExists("The connection " + key + " already exists.");
      this._connections.Add((object)key, (object)value);
    }

    /// <summary>
    /// Removes the connection matching the given key.
    /// 
    /// </summary>
    /// <param name="key"/>
    public void Remove(string key)
    {
      this._connections.Remove((object)key);
    }
  }
}
