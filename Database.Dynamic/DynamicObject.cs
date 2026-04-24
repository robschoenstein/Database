using System.ComponentModel;
using System.Diagnostics;
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Database.Dynamic.Exceptions;
using Database.Dynamic.Utils;

namespace Database.Dynamic
{
    public sealed class DynamicObject : IDynamicMetaObjectProvider, IDynamicObject, INotifyPropertyChanged
    {
        private static readonly MethodInfo DynamicObjectTryGetValue =
            typeof(RuntimeOps).GetMethod(nameof(RuntimeOps.DynamicTryGetValue))!;

        private static readonly MethodInfo DynamicTrySetValue =
            typeof(RuntimeOps).GetMethod(nameof(RuntimeOps.DynamicTrySetValue))!;

        private static readonly MethodInfo DynamicTryDeleteValue =
            typeof(RuntimeOps).GetMethod(nameof(RuntimeOps.DynamicTryDeleteValue))!;

        private static readonly MethodInfo DynamicPromoteClass =
            typeof(RuntimeOps).GetMethod(nameof(RuntimeOps.DynamicPromoteClass))!;

        private static readonly MethodInfo DynamicCheckVersion =
            typeof(RuntimeOps).GetMethod(nameof(RuntimeOps.DynamicCheckVersion))!;

        internal readonly object LockObject; // the read-only field is used for locking the Dynamic object
        private DynamicData _data; // the data currently being held by the Dynamic object
        private DynamicType _type; // the type currently being held by the Dynamic object
        private int _count; // the count of available members

        internal static readonly object
            Uninitialized = new object(); // A marker object used to identify that a value is uninitialized.

        internal const int
            AmbiguousMatchFound =
                -2; // The value is used to indicate there exists ambiguous match in the Dynamic object

        internal const int NoMatch = -1; // The value is used to indicate there is no matching member

        private PropertyChangedEventHandler? _propertyChanged;

        /// <summary>
        /// Creates a new DynamicObject with no members.
        /// </summary>
        public DynamicObject()
        {
            _data = DynamicData.Empty;
            LockObject = new object();
        }

        #region Get/Set/Delete Helpers

        /// <summary>
        /// Try to get the data stored for the specified class at the specified index.  If the
        /// class has changed a full lookup for the slot will be performed and the correct
        /// value will be retrieved.
        /// </summary>
        internal bool TryGetValue(object? indexClass, int index, string name, bool ignoreCase, out dynamic? value)
        {
            //TODO: value needs to be a nullable dynamic (dynamic?)

            // read the data now.  The data is immutable so we get a consistent view.
            // If there's a concurrent writer they will replace data and it just appears
            // that we won the race
            DynamicData data = _data;
            DynamicType type = _type;

            if (data.Class != indexClass || ignoreCase)
            {
                /* Re-search for the index matching the name here if
                 *  1) the class has changed, we need to get the correct index and return
                 *  the value there.
                 *  2) the search is case-insensitive:
                 *      a. the member specified by index may be deleted, but there might be other
                 *      members matching the name if the binder is case-insensitive.
                 *      b. the member that exactly matches the name didn't exist before and exists now,
                 *      need to find the exact match.
                 */
                index = data.Class.GetValueIndex(name, ignoreCase, this);
                if (index == DynamicObject.AmbiguousMatchFound)
                {
                    throw new AmbiguousMatchException($"Ambiguous match in DynamicObject: {name}");
                }
            }

            if (index == DynamicObject.NoMatch)
            {
                value = null;
                return false;
            }

            // Capture the value into a temp, so it doesn't get mutated after we check
            // for Uninitialized.
            object? temp = data[index];
            if (temp == Uninitialized)
            {
                value = null;
                return false;
            }

            // index is now known to be correct
            value = Convert.ChangeType(temp, type[index]);
            return true;
        }

        /// <summary>
        /// Sets the data for the specified class at the specified index.  If the class has
        /// changed then a full look for the slot will be performed.  If the new class does
        /// not have the provided slot then the Expando's class will change. Only case sensitive
        /// setter is supported in ExpandoObject.
        /// </summary>
        internal void TrySetValue(object? indexClass, int index, object? value, string name,
            Type valueType, bool ignoreCase, bool add)
        {
            DynamicData data;
            object? oldValue;

            DynamicType type;

            lock (LockObject)
            {
                data = _data;
                type = _type;


                if (data.Class != indexClass || ignoreCase)
                {
                    // The class has changed or we are doing a case-insensitive search,
                    // we need to get the correct index and set the value there.  If we
                    // don't have the value then we need to promote the class - that
                    // should only happen when we have multiple concurrent writers.
                    index = data.Class.GetValueIndex(name, ignoreCase, this);

                    if (index == DynamicObject.AmbiguousMatchFound)
                    {
                        throw new AmbiguousMatchException($"Ambiguous match in DynamicObject: {name}");
                    }

                    if (index == DynamicObject.NoMatch)
                    {
                        // Before creating a new class with the new member, need to check
                        // if there is the exact same member but is deleted. We should reuse
                        // the class if there is such a member.
                        int exactMatch = ignoreCase ? data.Class.GetValueIndexCaseSensitive(name) : index;
                        if (exactMatch != DynamicObject.NoMatch)
                        {
                            Debug.Assert(data[exactMatch] == Uninitialized);
                            index = exactMatch;
                        }
                        else
                        {
                            DynamicClass newClass = data.Class.FindNewClass(name);

                            var retVal = PromoteClassCore(data.Class, newClass);

                            data = retVal.Data;
                            type = retVal.Type;

                            // After the class promotion, there must be an exact match,
                            // so we can do case-sensitive search here.
                            index = data.Class.GetValueIndexCaseSensitive(name);
                            Debug.Assert(index != DynamicObject.NoMatch);
                        }
                    }
                }

                // Setting an uninitialized member increases the count of available members
                oldValue = data[index];
                if (oldValue == Uninitialized)
                {
                    _count++;
                }
                else if (add)
                {
                    throw new ArgumentException("Same key exists in DynamicObject.", name);
                }

                data[index] = value;
                type[index] = valueType;
            }

            // Notify property changed outside the lock
            PropertyChangedEventHandler? propertyChanged = _propertyChanged;
            if (propertyChanged != null && value != oldValue)
            {
                propertyChanged(this, new PropertyChangedEventArgs(data.Class.Names[index]));
            }
        }

        /// <summary>
        /// Deletes the data stored for the specified class at the specified index.
        /// </summary>
        internal bool TryDeleteValue(object? indexClass, int index, string name, bool ignoreCase, object? deleteValue)
        {
            DynamicData data;
            DynamicType type;

            lock (LockObject)
            {
                data = _data;
                type = _type;

                if (data.Class != indexClass || ignoreCase)
                {
                    // the class has changed or we are doing a case-insensitive search,
                    // we need to get the correct index.  If there is no associated index
                    // we simply can't have the value and we return false.
                    index = data.Class.GetValueIndex(name, ignoreCase, this);
                    if (index == DynamicObject.AmbiguousMatchFound)
                    {
                        throw new AmbiguousMatchException($"Ambiguous match in DynamicObject: {name}");
                        ;
                    }
                }

                if (index == DynamicObject.NoMatch)
                {
                    return false;
                }

                object? oldValue = data[index];
                if (oldValue == Uninitialized)
                {
                    return false;
                }

                // Make sure the value matches, if requested.
                //
                // It's a shame we have to call Equals with the lock held but
                // there doesn't seem to be a good way around that, and
                // ConcurrentDictionary in mscorlib does the same thing.
                if (deleteValue != Uninitialized && !object.Equals(oldValue, deleteValue))
                {
                    return false;
                }

                data[index] = Uninitialized;
                type[index] = Uninitialized.GetType();

                // Deleting an available member decreases the count of available members
                _count--;
            }

            // Notify property changed outside the lock
            _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(data.Class.Names[index]));

            return true;
        }

        /// <summary>
        /// Returns true if the member at the specified index has been deleted,
        /// otherwise false. Call this function holding the lock.
        /// </summary>
        internal bool IsDeletedMember(int index)
        {
            ContractUtils.AssertLockHeld(LockObject);
            Debug.Assert(index >= 0 && index <= _data.Length);

            if (index == _data.Length)
            {
                // The member is a newly added by SetMemberBinder and not in data yet
                return false;
            }

            return _data[index] == DynamicObject.Uninitialized;
        }

        /// <summary>
        /// Exposes the DynamicClass which we've associated with this
        /// Dynamic object.  Used for type checks in rules.
        /// </summary>
        internal DynamicClass Class => _data.Class;

        /// <summary>
        /// Promotes the class from the old type to the new type and returns the new
        /// DynamicData object.
        /// </summary>
        private PromoteClassCoreRetVal PromoteClassCore(DynamicClass oldClass, DynamicClass newClass)
        {
            Debug.Assert(oldClass != newClass);
            ContractUtils.AssertLockHeld(LockObject);

            if (_data.Class == oldClass)
            {
                _data = _data.UpdateClass(newClass);
                _type = _type.UpdateClass(newClass);
            }

            return new PromoteClassCoreRetVal
            {
                Data = _data,
                Type = _type
            };
        }

        /// <summary>
        /// Internal helper to promote a class.  Called from our RuntimeOps helper.  This
        /// version simply doesn't expose the DynamicData object which is a private
        /// data structure.
        /// </summary>
        internal void PromoteClass(object oldClass, object newClass)
        {
            lock (LockObject)
            {
                PromoteClassCore((DynamicClass)oldClass, (DynamicClass)newClass);
            }
        }

        #endregion

        #region IDynamicMetaObjectProvider Members

        DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(Expression parameter)
        {
            return new MetaExpando(parameter, this);
        }

        #endregion

        #region Helper methods

        private void TryAddMember(string name, object? value, Type type)
        {
            ArgumentNullException.ThrowIfNull(name);
            // Pass null to the class, which forces lookup.
            TrySetValue(null, -1, value, name, type, ignoreCase: false, add: true);
        }

        private bool TryGetValueForName(string key, out dynamic? value)
        {
            // Pass null to the class, which forces lookup.
            return TryGetValue(null, -1, key, ignoreCase: false, value: out value);
        }

        private bool DynamicContainsName(string key)
        {
            ContractUtils.AssertLockHeld(LockObject);
            return _data.Class.GetValueIndexCaseSensitive(key) >= 0;
        }

        // We create a non-generic type for the debug view for each different collection type
        // that uses DebuggerTypeProxy, instead of defining a generic debug view type and
        // using different instantiations. The reason for this is that support for generics
        // with using DebuggerTypeProxy is limited. For C#, DebuggerTypeProxy supports only
        // open types (from MSDN https://learn.microsoft.com/visualstudio/debugger/using-debuggertypeproxy-attribute).
        private sealed class NameCollectionDebugView
        {
            private readonly ICollection<string> _collection;

            public NameCollectionDebugView(ICollection<string> collection)
            {
                ArgumentNullException.ThrowIfNull(collection);
                _collection = collection;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public string[] Items
            {
                get
                {
                    string[] items = new string[_collection.Count];
                    _collection.CopyTo(items, 0);
                    return items;
                }
            }
        }

        [DebuggerTypeProxy(typeof(NameCollectionDebugView))]
        [DebuggerDisplay("Count = {Count}")]
        private sealed class NameCollection : ICollection<string>
        {
            private readonly DynamicObject _dynObj;
            private readonly int _dynObjVersion;
            private readonly int _dynPropCount;
            private readonly DynamicData _dynData;

            internal NameCollection(DynamicObject dynObj)
            {
                lock (dynObj.LockObject)
                {
                    _dynObj = dynObj;
                    _dynObjVersion = dynObj._data.Version;
                    _dynPropCount = dynObj._count;
                    _dynData = dynObj._data;
                }
            }

            private void CheckVersion()
            {
                if (_dynObj._data.Version != _dynObjVersion ||
                    _dynData != _dynObj._data)
                {
                    //the underlying expando object has changed
                    throw new NotSupportedException("Collection modified while enumerating");
                }
            }

            #region ICollection<string> Members

            public void Add(string item)
            {
                throw new NotSupportedException("Collection is read-only");
            }

            public void Clear()
            {
                throw new NotSupportedException("Collection is read-only");
            }

            public bool Contains(string item)
            {
                lock (_dynObj.LockObject)
                {
                    CheckVersion();
                    return _dynObj.DynamicContainsName(item);
                }
            }

            public void CopyTo(string[] array, int arrayIndex)
            {
                ArgumentNullException.ThrowIfNull(array);
                ContractUtils.RequiresArrayRange(array, arrayIndex, _dynPropCount,
                    nameof(arrayIndex), nameof(Count));

                lock (_dynObj.LockObject)
                {
                    CheckVersion();
                    DynamicData data = _dynObj._data;
                    for (int i = 0; i < data.Class.Names.Length; i++)
                    {
                        if (data[i] != Uninitialized)
                        {
                            array[arrayIndex++] = data.Class.Names[i];
                        }
                    }
                }
            }

            public int Count
            {
                get
                {
                    CheckVersion();
                    return _dynPropCount;
                }
            }

            public bool IsReadOnly => true;

            public bool Remove(string item)
            {
                throw new NotSupportedException("Collection is read-only");
            }

            #endregion

            #region IEnumerable<string> Members

            public IEnumerator<string> GetEnumerator()
            {
                for (int i = 0, n = _dynData.Class.Names.Length; i < n; i++)
                {
                    CheckVersion();
                    if (_dynData[i] != Uninitialized)
                    {
                        yield return _dynData.Class.Names[i];
                    }
                }
            }

            #endregion

            #region IEnumerable Members

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            #endregion
        }

        // We create a non-generic type for the debug view for each different collection type
        // that uses DebuggerTypeProxy, instead of defining a generic debug view type and
        // using different instantiations. The reason for this is that support for generics
        // with using DebuggerTypeProxy is limited. For C#, DebuggerTypeProxy supports only
        // open types (from MSDN https://learn.microsoft.com/visualstudio/debugger/using-debuggertypeproxy-attribute).
        private sealed class ValueCollectionDebugView
        {
            private readonly ICollection<object> _collection;

            public ValueCollectionDebugView(ICollection<object> collection)
            {
                ArgumentNullException.ThrowIfNull(collection);
                _collection = collection;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public object[] Items
            {
                get
                {
                    object[] items = new object[_collection.Count];
                    _collection.CopyTo(items, 0);
                    return items;
                }
            }
        }

        [DebuggerTypeProxy(typeof(ValueCollectionDebugView))]
        [DebuggerDisplay("Count = {Count}")]
        private sealed class ValueCollection : ICollection<object?>
        {
            private readonly DynamicObject _dynObj;
            private readonly int _dynObjVersion;
            private readonly int _dynPropCount;
            private readonly DynamicData _dynData;

            internal ValueCollection(DynamicObject dynObj)
            {
                lock (dynObj.LockObject)
                {
                    _dynObj = dynObj;
                    _dynObjVersion = dynObj._data.Version;
                    _dynPropCount = dynObj._count;
                    _dynData = dynObj._data;
                }
            }

            private void CheckVersion()
            {
                if (_dynObj._data.Version != _dynObjVersion ||
                    _dynData != _dynObj._data)
                {
                    //the underlying expando object has changed
                    throw new NotSupportedException("Collection modified while enumerating");
                }
            }

            #region ICollection<string> Members

            public void Add(object? item)
            {
                throw new NotSupportedException("Collection is read-only.");
            }

            public void Clear()
            {
                throw new NotSupportedException("Collection is read-only.");
            }

            public bool Contains(object? item)
            {
                lock (_dynObj.LockObject)
                {
                    CheckVersion();

                    DynamicData data = _dynObj._data;
                    for (int i = 0; i < data.Class.Names.Length; i++)
                    {
                        // See comment in TryDeleteValue; it's okay to call
                        // object.Equals with the lock held.
                        if (object.Equals(data[i], item))
                        {
                            return true;
                        }
                    }

                    return false;
                }
            }

            public void CopyTo(object?[] array, int arrayIndex)
            {
                ArgumentNullException.ThrowIfNull(array);

                //TODO: Replace this with the contents of ContractUtils.RequiresArrayRange
                ContractUtils.RequiresArrayRange(array, arrayIndex, _dynPropCount, nameof(arrayIndex), nameof(Count));

                lock (_dynObj.LockObject)
                {
                    CheckVersion();
                    DynamicData data = _dynObj._data;
                    for (int i = 0; i < data.Class.Names.Length; i++)
                    {
                        if (data[i] != Uninitialized)
                        {
                            array[arrayIndex++] = data[i];
                        }
                    }
                }
            }

            public int Count
            {
                get
                {
                    CheckVersion();
                    return _dynPropCount;
                }
            }

            public bool IsReadOnly => true;

            public bool Remove(object? item)
            {
                throw new NotSupportedException("Collection is read-only.");
            }

            #endregion

            #region IEnumerable<string> Members

            public IEnumerator<object?> GetEnumerator()
            {
                DynamicData data = _dynObj._data;
                for (int i = 0; i < data.Class.Names.Length; i++)
                {
                    CheckVersion();
                    // Capture the value into a temp so we don't inadvertently
                    // return Uninitialized.
                    object? temp = data[i];
                    if (temp != Uninitialized)
                    {
                        yield return temp;
                    }
                }
            }

            #endregion

            #region IEnumerable Members

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            #endregion
        }

        // We create a non-generic type for the debug view for each different collection type
        // that uses DebuggerTypeProxy, instead of defining a generic debug view type and
        // using different instantiations. The reason for this is that support for generics
        // with using DebuggerTypeProxy is limited. For C#, DebuggerTypeProxy supports only
        // open types (from MSDN https://learn.microsoft.com/visualstudio/debugger/using-debuggertypeproxy-attribute).
        private sealed class TypeCollectionDebugView
        {
            private readonly ICollection<Type> _collection;

            public TypeCollectionDebugView(ICollection<Type> collection)
            {
                ArgumentNullException.ThrowIfNull(collection);
                _collection = collection;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public Type[] Items
            {
                get
                {
                    Type[] items = new Type[_collection.Count];
                    _collection.CopyTo(items, 0);
                    return items;
                }
            }
        }

        [DebuggerTypeProxy(typeof(TypeCollectionDebugView))]
        [DebuggerDisplay("Count = {Count}")]
        private sealed class TypeCollection : ICollection<Type>
        {
            private readonly DynamicObject _dynObj;
            private readonly int _dynObjVersion;
            private readonly int _dynPropCount;
            private readonly DynamicType _dynType;

            internal TypeCollection(DynamicObject dynObj)
            {
                lock (dynObj.LockObject)
                {
                    _dynObj = dynObj;
                    _dynObjVersion = dynObj._data.Version;
                    _dynPropCount = dynObj._count;
                    _dynType = dynObj._type;
                }
            }

            private void CheckVersion()
            {
                if (_dynObj._data.Version != _dynObjVersion ||
                    _dynType != _dynObj._type)
                {
                    //the underlying expando object has changed
                    throw new NotSupportedException("Collection modified while enumerating");
                }
            }

            #region ICollection<string> Members

            public void Add(Type item)
            {
                throw new NotSupportedException("Collection is read-only.");
            }

            public void Clear()
            {
                throw new NotSupportedException("Collection is read-only.");
            }

            public bool Contains(Type item)
            {
                lock (_dynObj.LockObject)
                {
                    CheckVersion();

                    DynamicType type = _dynObj._type;
                    for (int i = 0; i < type.Class.Names.Length; i++)
                    {
                        // See comment in TryDeleteValue; it's okay to call
                        // object.Equals with the lock held.
                        if (object.Equals(type[i], item))
                        {
                            return true;
                        }
                    }

                    return false;
                }
            }

            public void CopyTo(Type[] array, int arrayIndex)
            {
                ArgumentNullException.ThrowIfNull(array);

                //TODO: Replace this with the contents of ContractUtils.RequiresArrayRange
                ContractUtils.RequiresArrayRange(array, arrayIndex, _dynPropCount,
                    nameof(arrayIndex), nameof(Count));

                lock (_dynObj.LockObject)
                {
                    CheckVersion();
                    DynamicType type = _dynObj._type;
                    for (int i = 0; i < type.Class.Names.Length; i++)
                    {
                        if (type[i] != Uninitialized)
                        {
                            array[arrayIndex++] = type[i];
                        }
                    }
                }
            }

            public int Count
            {
                get
                {
                    CheckVersion();
                    return _dynPropCount;
                }
            }

            public bool IsReadOnly => true;

            public bool Remove(Type item)
            {
                throw new NotSupportedException("Collection is read-only.");
            }

            #endregion

            #region IEnumerable<string> Members

            public IEnumerator<Type> GetEnumerator()
            {
                DynamicType type = _dynObj._type;
                for (int i = 0; i < type.Class.Names.Length; i++)
                {
                    CheckVersion();
                    // Capture the value into a temp so we don't inadvertently
                    // return Uninitialized.
                    Type temp = type[i];
                    if (temp != Uninitialized.GetType())
                    {
                        yield return temp;
                    }
                }
            }

            #endregion

            #region IEnumerable Members

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            #endregion
        }

        #endregion

        #region IDynamicObject Members

        ICollection<string> IDynamicObject.Names => new NameCollection(this);

        ICollection<dynamic?> IDynamicObject.Values => new ValueCollection(this);

        ICollection<Type> IDynamicObject.Types => new TypeCollection(this);

        dynamic? IDynamicObject.this[string name]
        {
            get
            {
                if (!TryGetValueForName(name, out dynamic? value))
                {
                    throw new NameNotFoundException(name);
                }

                return value;
            }
            set
            {
                ArgumentNullException.ThrowIfNull(name);

                var val = value;

                // Pass null to the class, which forces lookup.
                TrySetValue(null, -1, val, name, val?.GetType() ?? typeof(object), ignoreCase: false, add: false);
            }
        }

        void IDynamicObject.Add(string key, object? value)
        {
            this.TryAddMember(key, value, value?.GetType() ?? typeof(object));
        }

        void IDynamicObject.Add(string key, object? value, Type type)
        {
            this.TryAddMember(key, value, type);
        }

        bool IDynamicObject.ContainsName(string name)
        {
            ArgumentNullException.ThrowIfNull(name);

            DynamicData data = _data;
            int index = data.Class.GetValueIndexCaseSensitive(name);
            return index >= 0 && data[index] != Uninitialized;
        }

        bool IDynamicObject.Remove(string name)
        {
            ArgumentNullException.ThrowIfNull(name);
            // Pass null to the class, which forces lookup.
            return TryDeleteValue(null, -1, name, ignoreCase: false, deleteValue: Uninitialized);
        }

        bool IDynamicObject.TryGetValue(string key, out dynamic? value)
        {
            return TryGetValueForName(key, out value);
        }

        #endregion

        #region ICollection<KeyValuePair<string, object>> Members

        int ICollection<DynamicProperty>.Count => _count;

        bool ICollection<DynamicProperty>.IsReadOnly => false;

        void ICollection<DynamicProperty>.Add(DynamicProperty item)
        {
            TryAddMember(item.Name, item.Value, item.Type);
        }

        void ICollection<DynamicProperty>.Clear()
        {
            // We remove both class and data!
            DynamicData data;
            DynamicType type;

            lock (LockObject)
            {
                data = _data;
                type = _type;
                _data = DynamicData.Empty;
                _type = DynamicType.Empty;
                _count = 0;
            }

            // Notify property changed for all properties.
            var propertyChanged = _propertyChanged;
            if (propertyChanged != null)
            {
                for (int i = 0, n = data.Class.Names.Length; i < n; i++)
                {
                    if (data[i] != Uninitialized)
                    {
                        propertyChanged(this, new PropertyChangedEventArgs(data.Class.Names[i]));
                    }
                }
            }
        }

        bool ICollection<DynamicProperty>.Contains(DynamicProperty item)
        {
            //TODO: Need to add TryGetTypeForName
            if (!TryGetValueForName(item.Name, out object? value))
            {
                return false;
            }


            return object.Equals(value, item.Value); //TODO: && Type.Equals(type, item.Type)
        }

        void ICollection<DynamicProperty>.CopyTo(DynamicProperty[] array, int arrayIndex)
        {
            ArgumentNullException.ThrowIfNull(array);

            // We want this to be atomic and not throw, though we must do the range checks inside this lock.
            lock (LockObject)
            {
                ContractUtils.RequiresArrayRange(array, arrayIndex, _count, nameof(arrayIndex),
                    nameof(ICollection<KeyValuePair<string, object>>.Count));
                foreach (DynamicProperty item in this)
                {
                    array[arrayIndex++] = item;
                }
            }
        }

        bool ICollection<DynamicProperty>.Remove(DynamicProperty item)
        {
            return TryDeleteValue(null, -1, item.Name, ignoreCase: false, deleteValue: item.Value);
        }

        #endregion

        #region IEnumerable<KeyValuePair<string, object>> Member

        IEnumerator<DynamicProperty> IEnumerable<DynamicProperty>.GetEnumerator()
        {
            DynamicData data = _data;
            DynamicType type = _type;

            return GetDynamicEnumerator(data, data.Version, type, type.Version);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            DynamicData data = _data;
            DynamicType type = _type;

            return GetDynamicEnumerator(data, data.Version, type, type.Version);
        }

        // Note: takes the data and version as parameters so they will be
        // captured before the first call to MoveNext().
        private IEnumerator<DynamicProperty> GetDynamicEnumerator(DynamicData data, int dataVersion,
            DynamicType type, int typeVersion)
        {
            for (int i = 0; i < data.Class.Names.Length; i++)
            {
                if (_data.Version != dataVersion || data != _data
                                                 || _type.Version != typeVersion || type != _type)
                {
                    // The underlying dynamic object has changed:
                    // 1) the dataVersion of the dynamic data changed
                    // 2) the data object is changed
                    throw new InvalidOperationException("Collection was modified while enumerating");
                }

                // Capture the value into a temp so we don't inadvertently
                // return Uninitialized.
                object? tempData = data[i];
                Type tempType = type[i];

                if (tempData != Uninitialized)
                {
                    yield return new DynamicProperty(data.Class.Names[i], tempData, tempType);
                }
            }
        }

        #endregion

        #region MetaExpando

        private sealed class MetaExpando : DynamicMetaObject
        {
            public MetaExpando(Expression expression, DynamicObject value)
                : base(expression, BindingRestrictions.Empty, value)
            {
            }

            private DynamicMetaObject BindGetOrInvokeMember(DynamicMetaObjectBinder binder, string name,
                bool ignoreCase, DynamicMetaObject fallback, Func<DynamicMetaObject, DynamicMetaObject>? fallbackInvoke)
            {
                DynamicClass klass = Value.Class;

                //try to find the member, including the deleted members
                int index = klass.GetValueIndex(name, ignoreCase, Value);

                ParameterExpression value = Expression.Parameter(typeof(object), "value");

                Expression tryGetValue = Expression.Call(
                    DynamicObjectTryGetValue,
                    GetLimitedSelf(),
                    Expression.Constant(klass, typeof(object)),
                    StaticUtilities.Constant(index),
                    Expression.Constant(name),
                    StaticUtilities.Constant(ignoreCase),
                    value
                );

                var result = new DynamicMetaObject(value, BindingRestrictions.Empty);
                if (fallbackInvoke != null)
                {
                    result = fallbackInvoke(result);
                }

                result = new DynamicMetaObject(
                    Expression.Block(
                        new TrueReadOnlyCollection<ParameterExpression>(value),
                        new TrueReadOnlyCollection<Expression>(
                            Expression.Condition(
                                tryGetValue,
                                result.Expression,
                                fallback.Expression,
                                typeof(object)
                            )
                        )
                    ),
                    result.Restrictions.Merge(fallback.Restrictions)
                );

                return AddDynamicTestAndDefer(binder, Value.Class, null, result);
            }

            public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
            {
                ArgumentNullException.ThrowIfNull(binder);
                return BindGetOrInvokeMember(
                    binder,
                    binder.Name,
                    binder.IgnoreCase,
                    binder.FallbackGetMember(this),
                    null
                );
            }

            public override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
            {
                ArgumentNullException.ThrowIfNull(binder);
                return BindGetOrInvokeMember(
                    binder,
                    binder.Name,
                    binder.IgnoreCase,
                    binder.FallbackInvokeMember(this, args),
                    value => binder.FallbackInvoke(value, args, null)
                );
            }

            public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
            {
                ArgumentNullException.ThrowIfNull(binder);
                ArgumentNullException.ThrowIfNull(value);

                DynamicClass klass;
                int index;

                DynamicClass? originalClass =
                    GetClassEnsureIndex(binder.Name, binder.IgnoreCase, Value, out klass, out index);

                return AddDynamicTestAndDefer(
                    binder,
                    klass,
                    originalClass,
                    new DynamicMetaObject(
                        Expression.Call(
                            DynamicTrySetValue,
                            GetLimitedSelf(),
                            Expression.Constant(klass, typeof(object)),
                            StaticUtilities.Constant(index),
                            Expression.Convert(value.Expression, typeof(object)),
                            Expression.Constant(binder.Name),
                            StaticUtilities.Constant(binder.IgnoreCase)
                        ),
                        BindingRestrictions.Empty
                    )
                );
            }

            public override DynamicMetaObject BindDeleteMember(DeleteMemberBinder binder)
            {
                ArgumentNullException.ThrowIfNull(binder);

                int index = Value.Class.GetValueIndex(binder.Name, binder.IgnoreCase, Value);

                Expression tryDelete = Expression.Call(
                    DynamicTryDeleteValue,
                    GetLimitedSelf(),
                    Expression.Constant(Value.Class, typeof(object)),
                    StaticUtilities.Constant(index),
                    Expression.Constant(binder.Name),
                    StaticUtilities.Constant(binder.IgnoreCase)
                );
                DynamicMetaObject fallback = binder.FallbackDeleteMember(this);

                DynamicMetaObject target = new DynamicMetaObject(
                    Expression.IfThen(Expression.Not(tryDelete), fallback.Expression),
                    fallback.Restrictions
                );

                return AddDynamicTestAndDefer(binder, Value.Class, null, target);
            }

            public override IEnumerable<string> GetDynamicMemberNames()
            {
                var expandoData = Value._data;
                var klass = expandoData.Class;
                for (int i = 0; i < klass.Names.Length; i++)
                {
                    object? val = expandoData[i];
                    if (val != DynamicObject.Uninitialized)
                    {
                        yield return klass.Names[i];
                    }
                }
            }

            /// <summary>
            /// Adds a dynamic test which checks if the version has changed.  The test is only necessary for
            /// performance as the methods will do the correct thing if called with an incorrect version.
            /// </summary>
            private DynamicMetaObject AddDynamicTestAndDefer(DynamicMetaObjectBinder binder, DynamicClass klass,
                DynamicClass? originalClass, DynamicMetaObject succeeds)
            {
                Expression ifTestSucceeds = succeeds.Expression;
                if (originalClass != null)
                {
                    // we are accessing a member which has not yet been defined on this class.
                    // We force a class promotion after the type check.  If the class changes the
                    // promotion will fail and the set/delete will do a full lookup using the new
                    // class to discover the name.
                    Debug.Assert(originalClass != klass);

                    ifTestSucceeds = Expression.Block(
                        Expression.Call(
                            null,
                            DynamicPromoteClass,
                            GetLimitedSelf(),
                            Expression.Constant(originalClass, typeof(object)),
                            Expression.Constant(klass, typeof(object))
                        ),
                        succeeds.Expression
                    );
                }

                return new DynamicMetaObject(
                    Expression.Condition(
                        Expression.Call(
                            null,
                            DynamicCheckVersion,
                            GetLimitedSelf(),
                            Expression.Constant(originalClass ?? klass, typeof(object))
                        ),
                        ifTestSucceeds,
                        binder.GetUpdateExpression(ifTestSucceeds.Type)
                    ),
                    GetRestrictions().Merge(succeeds.Restrictions)
                );
            }

            /// <summary>
            /// Gets the class and the index associated with the given name.  Does not update the expando object.  Instead
            /// this returns both the original and desired new class.  A rule is created which includes the test for the
            /// original class, the promotion to the new class, and the set/delete based on the class post-promotion.
            /// </summary>
            private DynamicClass? GetClassEnsureIndex(string name, bool caseInsensitive, DynamicObject obj,
                out DynamicClass klass, out int index)
            {
                DynamicClass originalClass = Value.Class;

                index = originalClass.GetValueIndex(name, caseInsensitive, obj);
                if (index == DynamicObject.AmbiguousMatchFound)
                {
                    klass = originalClass;
                    return null;
                }

                if (index == DynamicObject.NoMatch)
                {
                    // go ahead and find a new class now...
                    DynamicClass newClass = originalClass.FindNewClass(name);

                    klass = newClass;
                    index = newClass.GetValueIndexCaseSensitive(name);

                    Debug.Assert(index != DynamicObject.NoMatch);
                    return originalClass;
                }
                else
                {
                    klass = originalClass;
                    return null;
                }
            }

            /// <summary>
            /// Returns our Expression converted to our known LimitType
            /// </summary>
            private Expression GetLimitedSelf()
            {
                if (StaticUtilities.AreEquivalent(Expression.Type, LimitType))
                {
                    return Expression;
                }

                return Expression.Convert(Expression, LimitType);
            }

            /// <summary>
            /// Returns a Restrictions object which includes our current restrictions merged
            /// with a restriction limiting our type
            /// </summary>
            private BindingRestrictions GetRestrictions()
            {
                Debug.Assert(Restrictions == BindingRestrictions.Empty,
                    "We don't merge, restrictions are always empty");

                return StaticUtilities.GetTypeRestriction(this);
            }

            public new DynamicObject Value => (DynamicObject)base.Value!;
        }

        #endregion

        #region PromoteClassCoreRetVal

        private sealed class PromoteClassCoreRetVal
        {
            internal DynamicData Data { get; set; }
            internal DynamicType Type { get; set; }
        }

        #endregion

        #region DynamicData

        /// <summary>
        /// Stores the class and the data associated with the class as one atomic
        /// pair.  This enables us to do a class check in a thread safe manner w/o
        /// requiring locks.
        /// </summary>
        private sealed class DynamicData
        {
            internal static readonly DynamicData Empty = new DynamicData();

            /// <summary>
            /// the dynamically assigned class associated with the Dynamic object
            /// </summary>
            internal readonly DynamicClass Class;

            /// <summary>
            /// data stored in the dynamic object, key names are stored in the class.
            ///
            /// Dynamic._data must be locked when mutating the value.  Otherwise a copy of it
            /// could be made and lose values.
            /// </summary>
            private readonly object?[] _dataArray;

            /// <summary>
            /// Indexer for getting/setting the data
            /// </summary>
            internal object? this[int index]
            {
                get { return _dataArray[index]; }
                set
                {
                    //when the array is updated, version increases, even the new value is the same
                    //as previous. Dictionary type has the same behavior.
                    _version++;
                    _dataArray[index] = value;
                }
            }

            internal int Version => _version;

            internal int Length => _dataArray.Length;

            /// <summary>
            /// Constructs an empty DynamicData object with the empty class and no data.
            /// </summary>
            private DynamicData()
            {
                Class = DynamicClass.Empty;
                _dataArray = Array.Empty<object>();
            }

            /// <summary>
            /// the version of the ExpandoObject that tracks set and delete operations
            /// </summary>
            private int _version;

            /// <summary>
            /// Constructs a new DynamicData object with the specified class and data.
            /// </summary>
            internal DynamicData(DynamicClass klass, object?[] data, int version)
            {
                Class = klass;
                _dataArray = data;
                _version = version;
            }

            /// <summary>
            /// Update the associated class and increases the storage for the data array if needed.
            /// </summary>
            internal DynamicData UpdateClass(DynamicClass newClass)
            {
                if (_dataArray.Length >= newClass.Names.Length)
                {
                    // we have extra space in our buffer, just initialize it to Uninitialized.
                    this[newClass.Names.Length - 1] = DynamicObject.Uninitialized;
                    return new DynamicData(newClass, _dataArray, _version);
                }
                else
                {
                    // we've grown too much - we need a new object array
                    int oldLength = _dataArray.Length;
                    object[] arr = new object[GetAlignedSize(newClass.Names.Length)];
                    Array.Copy(_dataArray, arr, _dataArray.Length);
                    DynamicData newData = new DynamicData(newClass, arr, _version);
                    newData[oldLength] = DynamicObject.Uninitialized;
                    return newData;
                }
            }

            private static int GetAlignedSize(int len)
            {
                // the alignment of the array for storage of values (must be a power of two)
                const int dataArrayAlignment = 8;

                // round up and then mask off lower bits
                return (len + (dataArrayAlignment - 1)) & (~(dataArrayAlignment - 1));
            }
        }

        #endregion

        #region DynamicType

        /// <summary>
        /// Stores the class and the data associated with the class as one atomic
        /// pair.  This enables us to do a class check in a thread safe manner w/o
        /// requiring locks.
        /// </summary>
        private sealed class DynamicType
        {
            internal static readonly DynamicType Empty = new DynamicType();

            /// <summary>
            /// the dynamically assigned class associated with the Expando object
            /// </summary>
            internal readonly DynamicClass Class;

            /// <summary>
            /// data stored in the expando object, key names are stored in the class.
            ///
            /// Expando._data must be locked when mutating the value.  Otherwise a copy of it
            /// could be made and lose values.
            /// </summary>
            private readonly Type[] _typesArray;

            /// <summary>
            /// Indexer for getting/setting the data
            /// </summary>
            internal Type this[int index]
            {
                get { return _typesArray[index]; }
                set
                {
                    //when the array is updated, version increases, even the new value is the same
                    //as previous. Dictionary type has the same behavior.
                    _version++;
                    _typesArray[index] = value;
                }
            }

            internal int Version => _version;

            internal int Length => _typesArray.Length;

            /// <summary>
            /// Constructs an empty DynamicType object with the empty class and no data.
            /// </summary>
            private DynamicType()
            {
                Class = DynamicClass.Empty;
                _typesArray = Array.Empty<Type>();
            }

            /// <summary>
            /// the version of the ExpandoObject that tracks set and delete operations
            /// </summary>
            private int _version;

            /// <summary>
            /// Constructs a new DynamicType object with the specified class and types.
            /// </summary>
            internal DynamicType(DynamicClass klass, Type[] types, int version)
            {
                Class = klass;
                _typesArray = types;
                _version = version;
            }

            /// <summary>
            /// Update the associated class and increases the storage for the data array if needed.
            /// </summary>
            internal DynamicType UpdateClass(DynamicClass newClass)
            {
                if (_typesArray.Length >= newClass.Names.Length)
                {
                    // we have extra space in our buffer, just initialize it to Uninitialized.
                    this[newClass.Names.Length - 1] = DynamicObject.Uninitialized.GetType();
                    return new DynamicType(newClass, _typesArray, _version);
                }
                else
                {
                    // we've grown too much - we need a new object array
                    int oldLength = _typesArray.Length;
                    Type[] arr = new Type[GetAlignedSize(newClass.Names.Length)];
                    Array.Copy(_typesArray, arr, _typesArray.Length);
                    DynamicType newType = new DynamicType(newClass, arr, _version);
                    newType[oldLength] = DynamicObject.Uninitialized.GetType();
                    return newType;
                }
            }

            private static int GetAlignedSize(int len)
            {
                // the alignment of the array for storage of values (must be a power of two)
                const int DataArrayAlignment = 8;

                // round up and then mask off lower bits
                return (len + (DataArrayAlignment - 1)) & (~(DataArrayAlignment - 1));
            }
        }

        #endregion

        #region INotifyPropertyChanged

        event PropertyChangedEventHandler? INotifyPropertyChanged.PropertyChanged
        {
            add { _propertyChanged += value; }
            remove { _propertyChanged -= value; }
        }

        #endregion
    }
}

namespace System.Runtime.CompilerServices
{
//
// Note: these helpers are kept as simple wrappers so they have a better
// chance of being inlined.
//
    public static partial class RuntimeOps
    {
        /// <summary>
        /// Gets the value of an item in an dynObj object.
        /// </summary>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="indexClass">The class of the dynObj object.</param>
        /// <param name="index">The index of the member.</param>
        /// <param name="name">The name of the member.</param>
        /// <param name="ignoreCase">true if the name should be matched ignoring case; false otherwise.</param>
        /// <param name="value">The out parameter containing the value of the member.</param>
        /// <returns>True if the member exists in the dynObj object, otherwise false.</returns>
        [Obsolete("RuntimeOps has been deprecated and is not supported.", error: true),
         EditorBrowsable(EditorBrowsableState.Never)]
        public static bool DynamicTryGetValue(Database.Dynamic.DynamicObject dynObj, object? indexClass, int index,
            string name, bool ignoreCase, out object? value)
        {
            return dynObj.TryGetValue(indexClass, index, name, ignoreCase, out value);
        }

        /// <summary>
        /// Sets the value of an item in an dynObj object.
        /// </summary>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="indexClass">The class of the dynObj object.</param>
        /// <param name="index">The index of the member.</param>
        /// <param name="value">The value of the member.</param>
        /// <param name="name">The name of the member.</param>
        /// <param name="ignoreCase">true if the name should be matched ignoring case; false otherwise.</param>
        /// <returns>
        /// Returns the index for the set member.
        /// </returns>
        [Obsolete("RuntimeOps has been deprecated and is not supported.", error: true),
         EditorBrowsable(EditorBrowsableState.Never)]
        public static object? DynamicTrySetValue(Database.Dynamic.DynamicObject dynObj, object? indexClass, int index,
            object? value, string name, Type valueType, bool ignoreCase)
        {
            dynObj.TrySetValue(indexClass, index, value, name, valueType, ignoreCase, false);
            return value;
        }

        /// <summary>
        /// Deletes the value of an item in an dynObj object.
        /// </summary>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="indexClass">The class of the dynObj object.</param>
        /// <param name="index">The index of the member.</param>
        /// <param name="name">The name of the member.</param>
        /// <param name="ignoreCase">true if the name should be matched ignoring case; false otherwise.</param>
        /// <returns>true if the item was successfully removed; otherwise, false.</returns>
        [Obsolete("RuntimeOps has been deprecated and is not supported.", error: true),
         EditorBrowsable(EditorBrowsableState.Never)]
        public static bool DynamicTryDeleteValue(Database.Dynamic.DynamicObject dynObj, object? indexClass, int index,
            string name, bool ignoreCase)
        {
            return dynObj.TryDeleteValue(indexClass, index, name, ignoreCase,
                Database.Dynamic.DynamicObject.Uninitialized);
        }

        /// <summary>
        /// Checks the version of the dynObj object.
        /// </summary>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="version">The version to check.</param>
        /// <returns>true if the version is equal; otherwise, false.</returns>
        [Obsolete("RuntimeOps has been deprecated and is not supported.", error: true),
         EditorBrowsable(EditorBrowsableState.Never)]
        public static bool DynamicCheckVersion(Database.Dynamic.DynamicObject dynObj, object? version)
        {
            return dynObj.Class == version;
        }

        /// <summary>
        /// Promotes an dynObj object from one class to a new class.
        /// </summary>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="oldClass">The old class of the dynObj object.</param>
        /// <param name="newClass">The new class of the dynObj object.</param>
        [Obsolete("RuntimeOps has been deprecated and is not supported.", error: true),
         EditorBrowsable(EditorBrowsableState.Never)]
        public static void DynamicPromoteClass(Database.Dynamic.DynamicObject dynObj, object oldClass, object newClass)
        {
            dynObj.PromoteClass(oldClass, newClass);
        }
    }
}