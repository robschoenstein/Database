using System.Threading;
using Database.Connection;

namespace Database
{
    /// <summary>
    /// Static database environment variable containing connection list
    ///
    /// It is initialized when the DataAccess class is constructed.
    /// </summary>
    internal static class Environment
    {
        //private static Environment _instance = null;
        private static readonly Lock Locker = new Lock();
        private static ConnectionList _connections;
        private static bool _isInitialized;

        /// <summary>
        /// Gets the ConnectionList
        /// </summary>
        public static ConnectionList Connections
        {
            get => Environment._connections;
            private set => Environment._connections = value;
        }

        public static bool IsInitialized
        {
            get => _isInitialized;
            private set => _isInitialized = value;
        }
        
        /// <summary>
        /// Initialize the Environment class
        /// </summary>
        /// <param name="defaultConnectionProperties">Default database connection properties</param>
        /// <returns></returns>
        public static void Initialize(ConnectionProperties defaultConnectionProperties)
        {
            //Remain thread safe
            lock (Locker)
            {
                Environment.Connections ??= new ConnectionList(defaultConnectionProperties);
                Environment.IsInitialized = true;
            }
        }
    }
}