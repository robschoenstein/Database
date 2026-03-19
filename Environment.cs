using System.Threading;
using Database.Connection;

namespace Database
{
    /// <summary>
    /// Static database environment variable containing connection list
    ///
    /// It is initialized during the initialization of the Data class
    /// </summary>
    internal static class Environment
    {
        //private static Environment _instance = null;
        private static readonly Lock Locker = new Lock();
        private static ConnectionList _connections;
        private static bool _initialized = false;

        /// <summary>
        /// Gets the ConnectionList
        /// </summary>
        public static ConnectionList Connections
        {
            get => Environment._connections;
            private set => Environment._connections = value;
        }

        public static bool Initialized
        {
            get => _initialized;
            set => _initialized = value;
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
                Environment.Initialized = true;
            }
        }
    }
}