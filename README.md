# Database Library

**Modern, high-performance data access layer for .NET 10**  
Supports **Microsoft SQL Server** and **PostgreSQL** with first-class entity mapping, Table-Valued Parameters, dynamic/ExpandoObject support, and full async APIs.

---

## Features

- Strongly-typed entity mapping from `DataTable`/`DataRow` with full attribute support (`[ColumnName]`, `[ChildColumnMap]`, `[InsertParamIgnore]`, `[UpdateParamIgnore]`, `[TVPIgnore]`, etc.)
- Dynamic/ExpandoObject support for both regular parameters and TVPs
- Table-Valued Parameters on SQL Server + JSONb fallback on PostgreSQL (composite array support planned)
- Full async API (`*Async` methods) with proper resource disposal
- Reflection caching via `ObjectCache` for maximum performance
- Multiple named connections with easy default switching
- Zero external licensed/GPL dependencies — pure Microsoft + Npgsql

## Quick Start

```csharp
// 1. Initialize once at startup
var props = new ConnectionProperties("default", "Server=...;Database=...", DbServerType.mssql);
var db = new DataAccess(props);

// 2. Strongly-typed usage
var employee = new Employee { FirstName = "John", ... };
var parameters = employee.ToInsertParameters();

await DataAccess.ExecuteAsync("InsertEmployee", parameters);

// 3. Dynamic usage (when you need it)
dynamic data = new ExpandoObject();
data.companyId = 42;
data.firstName = "Jane";

var dynamicParams = new Parameters();
dynamicParams.AddFromDynamic(data);

await DataAccess.ExecuteAsync("InsertEmployee", dynamicParams);
```

## Key Types

- `DataAccess` — main entry point (sync + async).
- `Parameters` — fluent parameter collection supporting Table Valued Parameters for MSSQL and JSONb for PostgreSQL + Dynamic/Expando Object support.
- `EntityCollection<T>` — modern replacement for the old `EntityCollectionBase<T>`
- `Extensions.ToInsertParameters<T>()`/`ToUpdateParameters<T>()` — Generates Insert/Update parameters from an entity (Use `[InsertParamIgnore]`/`[UpdateParamIgnore]` attributes to omit properties from parameter generation).

## Migration Notes (v1 -> V2)

- `EntityColectionBase<T>` is now **obsolete** — use `EntityCollection<T>` instead.
- Class name typo finally corrected: `Extentions` -> `Extensions`. By the time I realized I misspelled it, the library was already being used in more than a few enterprise projects. Sorry Intel, Wells Fargo, SRP, and everyone else.
- All reflection is finally cached, not just a few properties that have the original attributes. (`ObjectCache`) Reflection overhead has now been reduced significantly.
- `ChildColumnMap` attribute support is fully restored and more performant.
- Full async API added (recommended for new code).

## Building
`dotnet build -c Release`

**Target framework:** net10.0
----
I will update with a project that shows exactly how to use this library when I get some time to do so.~~~~
