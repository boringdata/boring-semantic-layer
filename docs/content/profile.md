# Backend Profiles

BSL provides a profile system for managing database connections using configuration files. Profiles let you:

- **Store backend configurations** for different environments (dev, staging, prod)
- **Share connections across systems** using global or local profile files
- **Switch backends easily** without changing your code
- **Secure credentials** with environment variable substitution


## Quick Start

### Python-Based

```python
from boring_semantic_layer import load_profile, to_semantic_table

# Load connection directly by profile name

con = load_profile('my_db')

# Load from a specific file
con = load_profile('my_db', profile_file='config/profiles.yml')

# Use the connection to access tables and create semantic tables
flights_table = con.table('flights')
flights = to_semantic_table(flights_table)
```

### YAML-Based

**File-level profile** - all tables from one connection:

```yaml
# flights_model.yml
profile: my_db

flights:
  table: flights
  dimensions:
    origin: _.origin
  measures:
    flight_count: _.count()
```

**Table-level profiles** - different tables from different connections:

```yaml
# multi_db_model.yml
flights:
  profile: postgres_db
  table: flights
  dimensions:
    origin: _.origin
  measures:
    flight_count: _.count()

carriers:
  profile: duckdb_db
  table: carriers
  dimensions:
    code: _.code
    name: _.name
```

```python
from boring_semantic_layer import from_yaml

# Profiles loaded automatically from YAML
models = from_yaml('multi_db_model.yml')

# Or pass profile as parameter
models = from_yaml('model.yml', profile='my_db')
```

## Profile YAML Format

Create a `profiles.yml` file in your project directory:

```yaml
dev_db:
  type: duckdb
  database: dev.db

prod_db:
  type: postgres
  host: ${POSTGRES_HOST}
  database: ${POSTGRES_DB}
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}

test_db:
  type: duckdb
  database: ":memory:"
  tables:
    flights: "data/flights.parquet"
```

**Notes:**
- The `type` field corresponds to the ibis backend name. Each backend has specific required parameters - see the [Supported Backends](#supported-backends) section below for details.
- Use `${VAR_NAME}` or `$VAR_NAME` syntax for environment variables (see `prod_db` example above for securing credentials).
- The `tables` configuration automatically loads parquet files for any backend that supports `read_parquet()` (DuckDB, Polars, DataFusion, etc.). An error will be raised if the backend doesn't support this feature.

### Auto-Loading Parquet Files for Testing

The `tables` configuration automatically creates database tables from parquet files when loading a profile:

```python
from boring_semantic_layer import load_profile

con = load_profile('test_db')  # Creates 'flights' table
print(con.list_tables())        # ['flights']
```

Supports both string paths and dict config:

```yaml
test_db:
  type: duckdb
  database: ":memory:"
  tables:
    # String format
    flights: "data/flights.parquet"

    # Dict format
    carriers:
      source: "data/carriers.parquet"
```

Ideal for testing, CI/CD, and prototyping. Supports local files, remote URLs, and S3 paths.

## Profile Resolution Order

`load_profile('my_db')` searches in this order:
1. `~/.config/bsl/profiles/my_db.yml` (BSL-specific profiles)
2. `./profiles.yml` (local project profiles)
3. xorq profiles directory (system-wide xorq profiles)

You can customize the search order:

```python
# Search only BSL directory
con = load_profile('my_db', search_locations=['bsl_dir'])

# Search only local directory
con = load_profile('my_db', search_locations=['local'])

# Custom order
con = load_profile('my_db', search_locations=['local', 'bsl_dir', 'xorq_dir'])
```

`from_yaml()` resolves profiles in order:
1. `profile` parameter
2. `BSL_PROFILE` environment variable
3. YAML `profile` section
4. Table-level `profile`

## Supported Backends

BSL accepts both native ibis backends and xorq's vendored ibis backends. The `type` field in your profile corresponds to the ibis backend name, and the other fields are passed as connection parameters.

**xorq's vendored backends are required to enable caching.** By default, BSL uses xorq's cached backends automatically via `load_profile()` for improved performance. If you need native ibis backends without caching, you can pass them directly to BSL functions.

See the [ibis backends documentation](https://ibis-project.org/backends/) for the complete list of supported backends and their required connection parameters.