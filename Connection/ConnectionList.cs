using System.Collections;
using Database.Exceptions;

namespace Database.Connection
{
    public class ConnectionList
    {
        private ConnectionProperties _defaultConnection;
        private Hashtable _connections = new Hashtable();

        public ConnectionList(ConnectionProperties connectionProperties)
        {
            _defaultConnection = connectionProperties;
            _connections.Add("default", _defaultConnection);
        }

        public ConnectionProperties DefaultConnection
        {
            get { return this._defaultConnection; }
            set
            {
                if (!this._connections.ContainsKey("default"))
                    throw new UnknownConnection("The default connection does not exist.");
                this._defaultConnection = value;
            }
        }

        public ConnectionProperties this[string key]
        {
            get
            {
                if (this._connections.ContainsKey(key))
                    return (ConnectionProperties)this._connections[key];
                else
                    throw new UnknownConnection("Unable to retrieve connection. The provided connection name '" + key +
                                                "' does not exist.");
            }
            set
            {
                if (!this._connections.ContainsKey(key))
                    throw new UnknownConnection("Unable to set connection. The provided connection name '" + key +
                                                "' does not exist.");
                this._connections[key] = value;
            }
        }

        /// <summary>
        /// Adds the key value pair to the Connection List
        /// 
        /// </summary>
        /// <param name="key"/><param name="value"/>
        public void Add(string key, ConnectionProperties value)
        {
            if (this._connections.ContainsKey(key))
                throw new ConnectionAlreadyExists("The connection " + key + " already exists.");
            this._connections.Add(key, value);
        }

        /// <summary>
        /// Removes the connection matching the given key.
        ///
        /// DO NOT ATTEMPT TO REMOVE THE DEFAULT CONNECTION!!! Update it instead.
        /// </summary>
        /// <param name="key">Connection Name</param>
        /// /// <exception cref="UnknownConnection">Thrown if the system attempts to delete the default connection.</exception>
        public void Remove(string key)
        {
            //DO NOT REMOVE DEFAULT CONNECTION. UPDATE IT!!!
            if (key == "default")
                throw new DefaultConnectionRemoval();

            this._connections.Remove(key);
        }

        /// <summary>
        /// Updates the connection matching the given key.
        /// </summary>
        /// <param name="key">Connection Name</param>
        /// <param name="value" type="ConnectionProperties">Connection Properties</param>
        /// <exception cref="UnknownConnection">Thrown if the connection does not exist</exception>
        public void Update(string key, ConnectionProperties value)
        {
            if (!this._connections.ContainsKey(key))
                throw new UnknownConnection("Unable to update connection. The provided connection name '" + key +
                                            "' does not exist.");
        }

        /// <summary>
        /// Clears all connections, except for the default connection.
        /// </summary>
        public void Clear()
        {
            foreach (string key in this._connections.Keys)
            {
                //NEVER remove the default connection.
                if (key == "default")
                {
                    continue;
                }

                _connections.Remove(key);
            }
        }
    }
}