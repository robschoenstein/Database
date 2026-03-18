namespace Database.Entity;

/// <summary>
/// A generic entity collection.
///
/// If you wish to create a custom collection, create a class and inherit from EntityCollectionBase/<T/>
/// </summary>
/// <typeparam name="T">Type to store in the entity collection</typeparam>
public class EntityCollection<T> : EntityCollectionBase<T>
{}