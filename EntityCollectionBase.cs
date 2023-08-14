using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using Database.Entity;

namespace Database
{
  public abstract class EntityCollectionBase<T> : IList<T>
  {
    #region Properties

    /// <summary>
    /// Gets or sets the element at the specified index.
    /// </summary>
    /// <param name="index">The index.</param>
    /// <returns>Item at the specified index.</returns>
    public T this[int index]
    {
      get { return EntityList[index]; }
      set { EntityList[index] = value; }
    }

    /// <summary>
    /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1" />.
    /// </summary>
    /// <returns>The number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1" />.</returns>
    public int Count
    {
      get { return EntityList.Count; }
    }

    /// <summary>
    /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only.
    /// </summary>
    /// <returns>true if the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only; otherwise, false.</returns>
    public bool IsReadOnly
    {
      get { return EntityList.IsReadOnly; }
    }

    /// <summary>
    /// The underlying <see cref="IList{T}"/>.
    /// </summary>
    /// <value>
    /// The underlying <see cref="IList{T}"/>.
    /// </value>
    protected IList<T> EntityList { get; set; }

    #endregion

    protected EntityCollectionBase()
    {
      EntityList = new List<T>();
    }

    /// <summary>
    /// Loads the specified <see cref="DataTable"/> into the underlying collection.
    /// </summary>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    protected virtual void Load(DataTable dataTable)
    {
      if (dataTable.Rows.Count < 1)
        return;

      if (EntityList == null)
        EntityList = new List<T>();
      else
        EntityList.Clear();

      AddRange(dataTable.ToEntities<T>());
    }

    /// <summary>
    /// Loads the specified <see cref="DataTable"/> into the underlying collection.
    /// </summary>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    /// <returns>The <see cref="DataTable"/>.</returns>
    protected virtual DataTable PartialLoad(DataTable dataTable)
    {
      if (dataTable.Rows.Count < 1)
        return dataTable;

      if (EntityList == null)
        EntityList = new List<T>();
      else
        EntityList.Clear();

      AddRange(dataTable.ToEntities<T>());

      return dataTable;
    }

    /// <summary>
    /// Removes the specified item.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <returns>Boolean indicating whether item has been removed.</returns>
    public bool Remove(T item)
    {
      return EntityList.Remove(item);
    }

    /// <summary>
    /// Removes all from collection.
    /// </summary>
    /// <param name="match">Match <see cref="Predicate{T}"/>.</param>
    /// <returns>Number of removed items.</returns>
    public int RemoveAll(Predicate<T> match)
    {
      var existing = new List<T>(EntityList);

      var value = existing.RemoveAll(match);

      EntityList = existing;

      return value;
    }

    /// <summary>
    /// Adds the specified entity.
    /// </summary>
    /// <param name="entity">The entity.</param>
    public void Add(T entity)
    {
      EntityList.Add(entity);
    }

    /// <summary>
    /// Clears the collection.
    /// </summary>
    public void Clear()
    {
      EntityList.Clear();
    }

    /// <summary>
    /// Determines whether the underlying list contains the specified item.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <returns>
    ///   <c>true</c> if [contains] [the specified item]; otherwise, <c>false</c>.
    /// </returns>
    public bool Contains(T item)
    {
      return EntityList.Contains(item);
    }

    /// <summary>
    /// Copies the elements of the underlying list to the specified array.
    /// </summary>
    /// <param name="array">The array.</param>
    /// <param name="arrayIndex">Index of the array.</param>
    public void CopyTo(T[] array, int arrayIndex)
    {
      EntityList.CopyTo(array, arrayIndex);
    }

    /// <summary>
    /// Adds the range.
    /// </summary>
    /// <param name="range">The range.</param>
    /// <exception cref="System.ArgumentNullException">range</exception>
    public void AddRange(IEnumerable<T> range)
    {
      if (range == null) throw new ArgumentNullException("range");

      foreach (T item in range)
        EntityList.Add(item);
    }

    /// <summary>
    /// Gets the enumerator.
    /// </summary>
    /// <returns></returns>
    public IEnumerator<T> GetEnumerator()
    {
      return EntityList.GetEnumerator();
    }

    /// <summary>
    /// Returns an enumerator that iterates through a collection.
    /// </summary>
    /// <returns>
    /// An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection.
    /// </returns>
    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    /// <summary>
    /// Gets the index of specified item.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <returns></returns>
    public int IndexOf(T item)
    {
      return EntityList.IndexOf(item);
    }

    /// <summary>
    /// Inserts item at the specified index.
    /// </summary>
    /// <param name="index">The index.</param>
    /// <param name="item">The item.</param>
    public void Insert(int index, T item)
    {
      EntityList.Insert(index, item);
    }

    /// <summary>
    /// Removes item at specified index.
    /// </summary>
    /// <param name="index">The index.</param>
    public void RemoveAt(int index)
    {
      EntityList.RemoveAt(index);
    }
  }
}
