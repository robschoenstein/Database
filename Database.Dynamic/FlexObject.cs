using System.Collections;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Database.Dynamic;
using Database.Dynamic.Exceptions;
using Database.Dynamic.Utils;

namespace Database.Dynamic
{
    /// <summary>
    /// Full-featured dynamic object that preserves original .NET type information for each property.
    /// Recommended for scenarios requiring strong type fidelity with dynamic behavior.
    /// Functions like ExpandoObject but with better type safety and improved thread safety.
    /// </summary>
    public sealed class FlexObject : IDynamicMetaObjectProvider, IFlexObject, INotifyPropertyChanged
    {
        internal readonly object LockObject = new();
        private readonly List<FlexProperty> _properties = new();
        private DynamicClass _class = DynamicClass.Empty;

        private PropertyChangedEventHandler? _propertyChanged;

        internal static readonly object Uninitialized = new();
        internal const int AmbiguousMatchFound = -2;
        internal const int NoMatch = -1;

        public FlexObject() { }

        #region Core Operations

        /// <summary>
        /// Tries to get a property value by name.
        /// </summary>
        internal bool InternalTryGetValue(string name, out dynamic? value)
        {
            var prop = _properties.Find(p => 
                p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            if (prop == null)
            {
                value = null;
                return false;
            }

            value = prop.GetValue();
            return true;
        }

        /// <summary>
        /// Sets or adds a property value. Automatically deduces type if not provided.
        /// </summary>
        internal void TrySetValue(string name, object? value, Type? explicitType = null)
        {
            var type = explicitType ?? value?.GetType() ?? typeof(object);

            lock (LockObject)
            {
                var index = _properties.FindIndex(p => 
                    p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

                if (index >= 0)
                {
                    _properties[index] = new FlexProperty(name, value, type);
                }
                else
                {
                    _properties.Add(new FlexProperty(name, value, type));
                    _class = _class.FindNewClass(name);
                }
            }

            _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }

        /// <summary>
        /// Deletes a property by name.
        /// </summary>
        internal bool TryDeleteValue(string name)
        {
            lock (LockObject)
            {
                var index = _properties.FindIndex(p => 
                    p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

                if (index < 0)
                {
                    return false;
                }

                _properties[index] = FlexProperty.Empty;

                return true;
            }
        }

        /// <summary>
        /// Checks if a member at the given index has been deleted.
        /// Must be called while holding LockObject.
        /// </summary>
        internal bool IsDeletedMember(int index)
        {
            // treat out-of-range as deleted for safety
            if (index < 0 || index >= _properties.Count)
            {
                return true;
            }

            return _properties[index] == null ||
                   _properties[index] == FlexProperty.Empty;
        }

        /// <summary>
        /// Returns the current FlexClass for this object.
        /// </summary>
        internal DynamicClass Class => _class;

        #endregion

        #region Public API (IFlexObject)

        public dynamic? this[string name]
        {
            get
            {
                if (!TryGetValue(name, out var value))
                {
                    throw new NameNotFoundException(name);
                }
                
                return value;
            }
            set => TrySetValue(name, value);
        }

        public void Add(string name, object? value) => 
            TrySetValue(name, value);
        public void Add(string name, object? value, Type type) => 
            TrySetValue(name, value, type);

        public bool Remove(string name) => TryDeleteValue(name);

        public bool TryGetValue(string name, [MaybeNullWhen(false)] out dynamic? value)
        {
            return InternalTryGetValue(name, out value);
        }

        public bool ContainsName(string name)
            => _properties.Exists(p => 
                p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

        public ICollection<string> Names => new NameCollection(this);
        public ICollection<object?> Values => new ValueCollection(this);
        public ICollection<Type> Types => new TypeCollection(this);

        #endregion

        #region Collection Implementation

        int ICollection<FlexProperty>.Count => _properties.Count;
        bool ICollection<FlexProperty>.IsReadOnly => false;

        void ICollection<FlexProperty>.Add(FlexProperty item)
        {
            ArgumentNullException.ThrowIfNull(item);
            
            lock (LockObject)
            {
                _properties.Add(item);
                _class = _class.FindNewClass(item.Name);
            }

            _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(item.Name));
        }

        void ICollection<FlexProperty>.Clear()
        {
            lock (LockObject)
            {
                _properties.Clear();
                _class = DynamicClass.Empty;
            }
        }

        bool ICollection<FlexProperty>.Contains(FlexProperty item)
        {
            ArgumentNullException.ThrowIfNull(item);
            return _properties.Contains(item);
        }

        void ICollection<FlexProperty>.CopyTo(FlexProperty[] array, int arrayIndex)
        {
            ArgumentNullException.ThrowIfNull(array);

            if (arrayIndex < 0 || arrayIndex > array.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            }

            if (array.Length - arrayIndex < _properties.Count)
            {
                throw new ArgumentException("The destination array is not long enough to copy all elements.",
                    nameof(array));
            }

            lock (LockObject)
            {
                _properties.CopyTo(array, arrayIndex);
            }
        }

        bool ICollection<FlexProperty>.Remove(FlexProperty item)
        {
            ArgumentNullException.ThrowIfNull(item);
            
            bool removed;
            
            lock (LockObject)
            {
                removed = _properties.Remove(item);
            }

            if (removed)
            {
                _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(item.Name));
            }
            
            return removed;
        }

        public IEnumerator<FlexProperty> GetEnumerator() => _properties.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        #endregion

        #region INotifyPropertyChanged

        event PropertyChangedEventHandler? INotifyPropertyChanged.PropertyChanged
        {
            add => _propertyChanged += value;
            remove => _propertyChanged -= value;
        }

        #endregion

        #region IDynamicMetaObjectProvider

        DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(Expression parameter)
            => new MetaDynamic(parameter, this);

        #endregion

        #region Nested Supporting Classes

        /// <summary>
        /// Internal class used to manage dynamic member names and fast lookup.
        /// Similar to ExpandoClass in the .NET runtime.
        /// </summary>
        internal sealed class DynamicClass
        {
            private readonly string[] _names;
            private readonly int _hashCode;
            private Dictionary<int, List<WeakReference>>? _transitions;

            internal static readonly DynamicClass Empty = new();

            internal string[] Names => _names;

            private DynamicClass()
            {
                _names = Array.Empty<string>();
                _hashCode = 6551;
            }

            internal DynamicClass(string[] names, int hashCode)
            {
                _names = names;
                _hashCode = hashCode;
            }

            internal DynamicClass FindNewClass(string newName)
            {
                //XOR _hashCode and the hash code of the new name together for new hash code
                int hashCode = _hashCode ^ newName.GetHashCode();

                lock (this)
                {
                    _transitions ??= new Dictionary<int, List<WeakReference>>();

                    if (!_transitions.TryGetValue(hashCode, out var list))
                    {
                        list = new List<WeakReference>();
                        _transitions[hashCode] = list;
                    }

                    for (int i = 0; i < list.Count; i++)
                    {
                        if (list[i].Target is DynamicClass klass)
                        {
                            if (string.Equals(klass._names[^1], newName, 
                                    StringComparison.Ordinal))
                            {
                                return klass;
                            }
                        }
                        else
                        {
                            list.RemoveAt(i--);
                        }
                    }

                    string[] newNames = new string[_names.Length + 1];
                    Array.Copy(_names, newNames, _names.Length);
                    //Access last item in array and set it to the new name (^1 corresponds to the final item)
                    newNames[^1] = newName;

                    var newClass = new DynamicClass(newNames, hashCode);
                    list.Add(new WeakReference(newClass));
                    return newClass;
                }
            }

            internal int GetValueIndexCaseSensitive(string name)
            {
                for (int i = 0; i < _names.Length; i++)
                {
                    if (string.Equals(_names[i], name, StringComparison.Ordinal))
                        return i;
                }

                return FlexObject.NoMatch;
            }

            internal int GetValueIndex(string name, bool ignoreCase, FlexObject obj)
            {
                if (!ignoreCase)
                {
                    return GetValueIndexCaseSensitive(name);
                }

                int match = FlexObject.NoMatch;
                for (int i = _names.Length - 1; i >= 0; i--)
                {
                    if (string.Equals(_names[i], name, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!obj.IsDeletedMember(i))
                        {
                            if (match == FlexObject.NoMatch)
                            {
                                match = i;
                            }
                            else
                            {
                                return FlexObject.AmbiguousMatchFound;
                            }
                        }
                    }
                }

                return match;
            }
        }

        /// <summary>
        /// Holds the actual property values.
        /// </summary>
        internal sealed class DynamicData
        {
            internal static readonly DynamicData Empty = new();

            internal readonly DynamicClass Class;
            private readonly object?[] _dataArray;
            private int _version;

            internal object? this[int index]
            {
                get => _dataArray[index];
                set
                {
                    _version++;
                    _dataArray[index] = value;
                }
            }

            internal int Version => _version;
            internal int Length => _dataArray.Length;

            private DynamicData()
            {
                Class = DynamicClass.Empty;
                _dataArray = Array.Empty<object>();
            }

            internal DynamicData(DynamicClass klass, object?[] data, int version)
            {
                Class = klass;
                _dataArray = data;
                _version = version;
            }

            internal DynamicData UpdateClass(DynamicClass newClass)
            {
                if (_dataArray.Length >= newClass.Names.Length)
                {
                    this[newClass.Names.Length - 1] = FlexObject.Uninitialized;
                    return new DynamicData(newClass, _dataArray, _version);
                }

                int oldLength = _dataArray.Length;
                object?[] arr = new object[GetAlignedSize(newClass.Names.Length)];
                Array.Copy(_dataArray, arr, oldLength);

                var newData = new DynamicData(newClass, arr, _version);
                newData[oldLength] = FlexObject.Uninitialized;
                return newData;
            }

            private static int GetAlignedSize(int len)
            {
                const int Alignment = 8;
                
                //Utilizing bitwise AND and NOT operators to help calculate aligned size
                return (len + Alignment - 1) & ~(Alignment - 1);
            }
        }

        /// <summary>
        /// Holds the type information for each property.
        /// </summary>
        internal sealed class DynamicType
        {
            internal static readonly DynamicType Empty = new();

            internal readonly DynamicClass Class;
            private readonly Type[] _typesArray;
            private int _version;

            internal Type this[int index]
            {
                get => _typesArray[index];
                set
                {
                    _version++;
                    _typesArray[index] = value;
                }
            }

            internal int Version => _version;
            internal int Length => _typesArray.Length;

            private DynamicType()
            {
                Class = DynamicClass.Empty;
                _typesArray = Array.Empty<Type>();
            }

            internal DynamicType(DynamicClass klass, Type[] types, int version)
            {
                Class = klass;
                _typesArray = types;
                _version = version;
            }

            internal DynamicType UpdateClass(DynamicClass newClass)
            {
                if (_typesArray.Length >= newClass.Names.Length)
                {
                    this[newClass.Names.Length - 1] = typeof(object);
                    return new DynamicType(newClass, _typesArray, _version);
                }

                int oldLength = _typesArray.Length;
                Type[] arr = new Type[GetAlignedSize(newClass.Names.Length)];
                Array.Copy(_typesArray, arr, oldLength);

                var newType = new DynamicType(newClass, arr, _version);
                newType[oldLength] = typeof(object);
                return newType;
            }

            private static int GetAlignedSize(int len)
            {
                const int Alignment = 8;
                
                //Utilizing bitwise AND and NOT operators to help calculate aligned size
                return (len + Alignment - 1) & ~(Alignment - 1);
            }
        }

        #region MetaDynamic (Dynamic Binding Support)

        /// <summary>
        /// Provides dynamic member binding for FlexObject (GetMember, SetMember, etc.).
        /// </summary>
        private sealed class MetaDynamic : DynamicMetaObject
        {
            public MetaDynamic(Expression expression, FlexObject value)
                : base(expression, BindingRestrictions.Empty, value)
            {
            }

            private FlexObject FlexObject => (FlexObject)Value!;

            public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
            {
                ArgumentNullException.ThrowIfNull(binder);
                return BindGetOrInvoke(binder.Name, binder.IgnoreCase, 
                    binder.FallbackGetMember(this), null);
            }

            public override DynamicMetaObject BindSetMember(SetMemberBinder binder, 
                DynamicMetaObject value)
            {
                ArgumentNullException.ThrowIfNull(binder);
                ArgumentNullException.ThrowIfNull(value);

                var setExpr = Expression.Call(
                    Expression.Convert(Expression, typeof(FlexObject)),
                    typeof(FlexObject).GetMethod(nameof(FlexObject.TrySetValue))!,
                    Expression.Constant(binder.Name),
                    Expression.Convert(value.Expression, typeof(object)),
                    Expression.Constant(null, typeof(Type))
                );

                return new DynamicMetaObject(setExpr, GetRestrictions());
            }

            public override DynamicMetaObject BindDeleteMember(DeleteMemberBinder binder)
            {
                ArgumentNullException.ThrowIfNull(binder);

                var deleteExpr = Expression.Call(
                    Expression.Convert(Expression, typeof(FlexObject)),
                    typeof(FlexObject).GetMethod(nameof(FlexObject.TryDeleteValue))!,
                    Expression.Constant(binder.Name)
                );

                var fallback = binder.FallbackDeleteMember(this);

                return new DynamicMetaObject(
                    Expression.IfThen(Expression.Not(deleteExpr), fallback.Expression),
                    fallback.Restrictions);
            }

            public override IEnumerable<string> GetDynamicMemberNames()
            {
                return FlexObject.Names;
            }

            private DynamicMetaObject BindGetOrInvoke(string name, bool ignoreCase,
                DynamicMetaObject fallback, Func<DynamicMetaObject, DynamicMetaObject>? fallbackInvoke)
            {
                var valueParam = Expression.Parameter(typeof(object), "value");

                var tryGet = Expression.Call(
                    Expression.Convert(Expression, typeof(FlexObject)),
                    typeof(FlexObject).GetMethod(nameof(FlexObject.InternalTryGetValue))!,
                    Expression.Constant(name),
                    valueParam
                );

                var result = new DynamicMetaObject(valueParam, BindingRestrictions.Empty);
                
                if (fallbackInvoke != null)
                {
                    result = fallbackInvoke(result);
                }

                var block = Expression.Block(
                    new[] { valueParam },
                    Expression.Condition(tryGet, result.Expression, fallback.Expression, typeof(object))
                );

                return new DynamicMetaObject(block, GetRestrictions().Merge(fallback.Restrictions));
            }

            private BindingRestrictions GetRestrictions()
            {
                return BindingRestrictions.GetTypeRestriction(Expression, LimitType);
            }
        }

        #endregion

        #region Collection Views (Snapshot-based for thread safety)

        private sealed class NameCollection : ICollection<string>
        {
            private readonly string[] _snapshot;

            internal NameCollection(FlexObject obj)
            {
                lock (obj.LockObject)
                {
                    _snapshot = obj._properties.Select(p => p.Name).ToArray();
                }
            }

            public int Count => _snapshot.Length;
            public bool IsReadOnly => true;

            public IEnumerator<string> GetEnumerator() => ((IEnumerable<string>)_snapshot).GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public bool Contains(string item) =>
                Array.Exists(_snapshot, s => s.Equals(item, StringComparison.OrdinalIgnoreCase));

            public void CopyTo(string[] array, int arrayIndex) => _snapshot.CopyTo(array, arrayIndex);

            public void Add(string item) => throw new NotSupportedException("Collection is read-only.");
            public void Clear() => throw new NotSupportedException("Collection is read-only.");
            public bool Remove(string item) => throw new NotSupportedException("Collection is read-only.");
        }

        private sealed class ValueCollection : ICollection<object?>
        {
            private readonly object?[] _snapshot;

            internal ValueCollection(FlexObject obj)
            {
                lock (obj.LockObject)
                {
                    _snapshot = obj._properties.Select(p => p.Value).ToArray();
                }
            }

            public int Count => _snapshot.Length;
            public bool IsReadOnly => true;

            public IEnumerator<dynamic?> GetEnumerator() => ((IEnumerable<dynamic?>)_snapshot).GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public bool Contains(object? item) => Array.Exists(_snapshot, v => Equals(v, item));
            public void CopyTo(dynamic?[] array, int arrayIndex) => _snapshot.CopyTo(array, arrayIndex);

            public void Add(dynamic? item) => throw new NotSupportedException("Collection is read-only.");
            public void Clear() => throw new NotSupportedException("Collection is read-only.");
            public bool Remove(dynamic? item) => throw new NotSupportedException("Collection is read-only.");
        }

        private sealed class TypeCollection : ICollection<Type>
        {
            private readonly Type[] _snapshot;

            internal TypeCollection(FlexObject obj)
            {
                lock (obj.LockObject)
                {
                    _snapshot = obj._properties.Select(p => p.Type).ToArray();
                }
            }

            public int Count => _snapshot.Length;
            public bool IsReadOnly => true;

            public IEnumerator<Type> GetEnumerator() => ((IEnumerable<Type>)_snapshot).GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public bool Contains(Type item) => Array.Exists(_snapshot, t => t == item);
            public void CopyTo(Type[] array, int arrayIndex) => _snapshot.CopyTo(array, arrayIndex);

            public void Add(Type item) => throw new NotSupportedException("Collection is read-only.");
            public void Clear() => throw new NotSupportedException("Collection is read-only.");
            public bool Remove(Type item) => throw new NotSupportedException("Collection is read-only.");
        }

        #endregion

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
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static bool DynamicTryGetValue(FlexObject dynObj, object? indexClass, int index,
            string name, bool ignoreCase, out object? value)
        {
            if (dynObj.InternalTryGetValue(name, out var dynamicValue))
            {
                value = dynamicValue;
                return true;
            }

            value = null;
            return false;
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
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static object? DynamicTrySetValue(FlexObject dynObj, object? indexClass, int index,
            object? value, string name, Type? valueType, bool ignoreCase)
        {
            dynObj.TrySetValue(name, value, valueType);
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
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static bool DynamicTryDeleteValue(FlexObject dynObj, object? indexClass, int index,
            string name, bool ignoreCase)
        {
            return dynObj.TryDeleteValue(name);
        }

        /// <summary>
        /// Checks the version of the dynObj object.
        /// </summary>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="version">The version to check.</param>
        /// <returns>true if the version is equal; otherwise, false.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static bool DynamicCheckVersion(FlexObject dynObj, object? version)
        {
            //return dynObj.Class == version;
            return ReferenceEquals(dynObj.Class, version);
        }

        /// <summary>
        /// Promotes an dynObj object from one class to a new class.
        /// </summary>
        /// <remarks>
        /// This method does nothing. It's only here for IDynamicMetaObjectProvider support
        /// </remarks>
        /// <param name="dynObj">The dynObj object.</param>
        /// <param name="oldClass">The old class of the dynObj object.</param>
        /// <param name="newClass">The new class of the dynObj object.</param>
        [Obsolete("RuntimeOps has been deprecated and is not supported.", error: true),
         EditorBrowsable(EditorBrowsableState.Never)]
        public static void DynamicPromoteClass(Database.Dynamic.FlexObject dynObj, object oldClass, object newClass)
        {
            // No operation needed — class promotion happens inside TrySetValue
        }
    }
}