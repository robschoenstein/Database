using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Database.Exceptions;

namespace Database.Connection
{
    /// <summary>
    /// Thread-safe collection of database connection properties.
    /// Manages the default connection and any additional named connections.
    /// </summary>
    public class ConnectionList
    {
        private readonly Dictionary<string, ConnectionProperties> _connections = new(StringComparer.OrdinalIgnoreCase);
        private ConnectionProperties _defaultConnection = null!;

        /// <summary>
        /// Creates the connection list with the default connection.
        /// </summary>
        public ConnectionList(ConnectionProperties defaultConnectionProperties)
        {
            ArgumentNullException.ThrowIfNull(defaultConnectionProperties);
            
            _defaultConnection = defaultConnectionProperties;
            _connections.Add("default", _defaultConnection);
        }

        /// <summary>
        /// Gets or sets the default connection.
        /// </summary>
        public ConnectionProperties DefaultConnection
        {
            get => _defaultConnection;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                
                if (!_connections.ContainsKey("default"))
                {
                    throw new UnknownConnection("The default connection does not exist.");
                }

                _defaultConnection = value;
                _connections["default"] = value;
            }
        }

        // <summary>
        /// Gets or sets a connection by name (case-insensitive).
        /// </summary>
        public ConnectionProperties this[string key]
        {
            get
            {
                if (string.IsNullOrEmpty(key))
                {
                    throw new UnknownConnection("Connection key/name cannot be empty.");
                }

                if (!_connections.TryGetValue(key, out var connection))
                {
                    throw new UnknownConnection(
                        $"Unable to retrieve connection. The provided connection name '{key}' does not exist.");
                }

                return connection;
            }
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                
                if (string.IsNullOrEmpty(key))
                {
                    throw new UnknownConnection("Connection key/name cannot be empty.");
                }

                if (!_connections.ContainsKey(key))
                {
                    throw new UnknownConnection(
                        $"Unable to set connection. The provided connection name '{key}' does not exist.");
                }

                _connections[key] = value;
            }
        }

        /// <summary>
        /// Adds the key value pair to the Connection List
        /// 
        /// </summary>
        /// <param name="key"/><param name="value"/>
        public void Add(string key, ConnectionProperties value)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(value);
            
            if (_connections.ContainsKey(key))
            {
                throw new ConnectionAlreadyExists($"The connection '{key}' already exists.");
            }
            
            _connections.Add(key, value);
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
            ArgumentNullException.ThrowIfNull(key);
            
            //DO NOT REMOVE DEFAULT CONNECTION. UPDATE IT!!!
            if (key.Equals("default", StringComparison.OrdinalIgnoreCase))
            {
                throw new DefaultConnectionRemoval();
            }

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
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(value);
            
            if (!_connections.ContainsKey(key))
            {
                throw new UnknownConnection($"Unable to update connection. The provided connection name '{key}' does not exist.");
            }
        }

        /// <summary>
        /// Clears all connections, except for the default connection.
        /// </summary>
        public void Clear()
        {
            //Get all items that are NOT the default connection
            var itemsToRemove = _connections
                .Where(c => c.Key != "default").ToList();
            
            foreach (var item in itemsToRemove)
            {
                _connections.Remove(item.Key);
            }
        }
    }
}