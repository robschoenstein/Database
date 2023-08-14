namespace Database
{
  public static class Environment
  {
    private static ConnectionList _connections = new ConnectionList();

    public static ConnectionList Connections
    {
      get
      {
        return Environment._connections;
      }
    }

    public static bool IsWindowsApplication { get; set; }

    static Environment()
    {
    }
  }
}
