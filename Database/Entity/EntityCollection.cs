using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace Database.Entity;

/// <summary>
/// A generic entity collection.
/// </summary>
/// <typeparam name="T">Type to store in the entity collection</typeparam>
public class EntityCollection<T> : IEntityCollection<T>
{
    protected IList<T> Entities { get; set; } = new List<T>();

    /// <summary>
    /// Gets or sets the element at the specified index.
    /// </summary>
    /// <param name="index">The index.</param>
    /// <returns>Item at the specified index.</returns>
    public T this[int index]
    {
        get => Entities[index];
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            
            Entities[index] = value;
        }
    }

    /// <summary>
    /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1" />.
    /// </summary>
    /// <returns>The number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1" />.</returns>
    public int Count => Entities.Count;

    /// <summary>
    /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only.
    /// </summary>
    /// <returns>true if the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only; otherwise, false.</returns>
    public bool IsReadOnly => Entities.IsReadOnly;

    /// <summary>
    /// Gets a value indicating whether the collection is a collection of System.Collections.IList objects.
    /// </summary>
    public bool ContainsListCollection => true;

    /// <summary>
    /// Initializes the underlying list
    /// </summary>
    protected EntityCollection()
    {
    }

    /// <summary>
    /// Returns an enumerator that iterates through a collection.
    /// </summary>
    /// <returns>
    /// An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection.
    /// </returns>
    public IEnumerator<T> GetEnumerator() => Entities.GetEnumerator();

    /// <summary>
    /// Returns an enumerator that iterates through a collection.
    /// </summary>
    /// <returns>
    /// An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection.
    /// </returns>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Adds the specified entity.
    /// </summary>
    /// <param name="entity">The entity.</param>
    public void Add(T entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        
        Entities.Add(entity);
    }

    /// <summary>
    /// Clears the underlying list.
    /// </summary>
    public void Clear() => Entities.Clear();

    /// <summary>
    /// Determines whether the underlying list contains the specified item.
    /// </summary>
    /// <param name="entity">The entity.</param>
    /// <returns>
    ///   <c>true</c> if [contains] [the specified item]; otherwise, <c>false</c>.
    /// </returns>
    public bool Contains(T entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        
        return Entities.Contains(entity);
    }

    /// <summary>
    /// Copies the elements of the underlying list to the specified array.
    /// </summary>
    /// <param name="array">The array.</param>
    /// <param name="arrayIndex">Index of the array.</param>
    public void CopyTo(T[] array, int arrayIndex) => Entities.CopyTo(array, arrayIndex);

    /// <summary>
    /// Removes the specified entity.
    /// </summary>
    /// <param name="entity">The entity.</param>
    /// <returns><c>true</c> if [the specified entity] has been removed; otherwise, <c>false</c>.</returns>
    public bool Remove(T entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        
        return Entities.Remove(entity);
    }

    /// <summary>
    /// Gets the index of specified entity.
    /// </summary>
    /// <param name="entity">The entity.</param>
    /// <returns>Index of the entity</returns>
    public int IndexOf(T entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        
        return Entities.IndexOf(entity);
    }

    /// <summary>
    /// Inserts entity at the specified index.
    /// </summary>
    /// <param name="index">The index.</param>
    /// <param name="entity">The entity.</param>
    public void Insert(int index, T entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        
        Entities.Insert(index, entity);
    }

    /// <summary>
    /// Removes entity at the specified index.
    /// </summary>
    /// <param name="index">The index.</param>
    public void RemoveAt(int index) => Entities.RemoveAt(index);

    /// <summary>
    /// Removes all specified items from the underlying list.
    /// </summary>
    /// <param name="match">Match <see cref="Predicate{T}"/>.</param>
    /// <returns>Number of removed items.</returns>
    public int RemoveAll(Predicate<T> match)
    {
        var existing = new List<T>(Entities);
        var value = existing.RemoveAll(match);

        Entities = existing;

        return value;
    }

    /// <summary>
    /// Adds range of <see cref="T:System.Collections.IEnumerable" /> to the underlying list.
    /// </summary>
    /// <param name="range"><see cref="T:System.Collections.IEnumerable" /> range.</param>
    /// <exception cref="System.ArgumentNullException">range</exception>
    public void AddRange(IEnumerable<T> range)
    {
        ArgumentNullException.ThrowIfNull(range);

        foreach (T entity in range)
        {
            Entities.Add(entity);
        }
    }

    /// <summary>
    /// Clears the underlying list and adds the specified <see cref="DataTable"/> into the underlying collection.
    /// </summary>
    /// <param name="dataTable">The <see cref="DataTable"/>.</param>
    protected virtual void Load(DataTable dataTable)
    {
        ArgumentNullException.ThrowIfNull(dataTable);
        
        if (dataTable.Rows.Count < 1)
        {
            return;
        }

        Entities.Clear();

        AddRange(dataTable.ToEntities<T>());
    }
    
    /// <summary>
    /// Returns the underlying <see cref="IList"/> of entities
    /// </summary>
    /// <returns>Underlying <see cref="IList"/> of entities</returns>
    public IList GetList() => Entities.ToList();
}