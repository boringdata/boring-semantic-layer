# Profile System Documentation

BSL supports xorq's profile system for managing database connections, with additional support for local `profile.yml` files for project-specific configurations.

## Overview

Profiles provide a secure, reusable way to manage database connections:

- **Security**: Store credentials as environment variables
- **Reusability**: Create once, use everywhere
- **Consistency**: Single source of truth for connection parameters
- **Organization**: Separate connection config from semantic models

## Profile Resolution Order

When you call `load_profile('profile_name')`, BSL searches in this order:

1. **Direct file path**: If `profile_name` ends with `.yml`/`.yaml` and exists, load from that file
2. **Explicit profile_file**: If `profile_file` parameter is provided, load from that file
3. **Local profile.yml**: If `./profile.yml` exists in current directory, load from there
4. **Xorq saved profiles**: Load from `~/.config/xorq/profiles/`

## Usage Methods

### Method 1: Local profile.yml File (Recommended for Projects)

Create a `profile.yml` file in your project root:

```yaml
# profile.yml
dev_db:
  type: duckdb
  database: dev.db

prod_db:
  type: postgres
  host: ${POSTGRES_HOST}
  port: 5432
  database: ${POSTGRES_DB}
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
```

Then use it in your code:

```python
from boring_semantic_layer.profile import load_profile

# Automatically finds ./profile.yml
con = load_profile('dev_db')
```

Or in YAML configurations:

```yaml
# models.yml
profile: dev_db

flights:
  table: flights_tbl
  dimensions: {...}
  measures: {...}
```

**Benefits**:
- No setup required - just create the file
- Project-specific configurations
- Can be committed to git (with env vars for secrets)
- Easy to understand and modify

### Method 2: Xorq Saved Profiles (Recommended for Personal/System-Wide)

Save profiles using xorq (one-time setup):

```python
import xorq.api as xo

# Create connection
con = xo.duckdb.connect("my_database.db")

# Load some data
flights = con.read_parquet("flights.parquet")
con.create_table("flights_tbl", flights)

# Save profile for reuse
con._profile.save(alias='my_duckdb')
```

Then use it:

```python
from boring_semantic_layer.profile import load_profile

# Loads from ~/.config/xorq/profiles/
con = load_profile('my_duckdb')
```

**Benefits**:
- System-wide profiles shared across projects
- Managed by xorq
- Support for all xorq backends
- Profile versioning and management

### Method 3: Direct File Path

Load directly from a specific file:

```python
# Load profile from specific file
con = load_profile('./config/my_profile.yml')

# Or specify profile name in a file
con = load_profile('dev_db', profile_file='config/profiles.yml')
```

## Profile File Format

Profile files use YAML format with this structure:

```yaml
profile_name:
  type: backend_type
  param1: value1
  param2: ${ENV_VAR}
  param3: value3
```

### Supported Backends

Any ibis-supported backend can be used:

#### DuckDB
```yaml
my_duckdb:
  type: duckdb
  database: path/to/database.db  # or ":memory:"
```

#### PostgreSQL
```yaml
my_postgres:
  type: postgres
  host: localhost
  port: 5432
  database: mydb
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
```

#### Snowflake
```yaml
my_snowflake:
  type: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  user: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  database: ${SNOWFLAKE_DATABASE}
  warehouse: ${SNOWFLAKE_WAREHOUSE}
  schema: PUBLIC
```

#### BigQuery
```yaml
my_bigquery:
  type: bigquery
  project_id: ${GCP_PROJECT}
  dataset_id: my_dataset
```

## Environment Variables

Use environment variables for sensitive information:

```yaml
prod_db:
  type: postgres
  host: ${POSTGRES_HOST}
  password: ${POSTGRES_PASSWORD}
```

Supported formats:
- `${VAR_NAME}` - preferred
- `$VAR_NAME` - also supported

Set environment variables before running:

```bash
export POSTGRES_HOST=prod.example.com
export POSTGRES_PASSWORD=secret
```

## Using Profiles in YAML Configurations

### Simple Format

```yaml
# models.yml
profile: my_duckdb

flights:
  table: flights_tbl
  dimensions:
    origin: _.origin
  measures:
    flight_count: _.count()
```

### Extended Format (Specify Tables)

```yaml
# models.yml
profile:
  name: my_duckdb
  tables: [flights_tbl, carriers_tbl]  # Optional: load only these tables

flights:
  table: flights_tbl
  dimensions:
    origin: _.origin
  measures:
    flight_count: _.count()
```

## Python API

### Load Connection

```python
from boring_semantic_layer.profile import load_profile

# Load from any source (auto-resolves)
con = load_profile('my_profile')

# Load from specific file
con = load_profile('my_profile', profile_file='config/profiles.yml')

# List available tables
print(con.list_tables())

# Use connection directly
table = con.table('my_table')
```

### Load Tables from Profile

```python
from boring_semantic_layer.profile import load_tables_from_profile

# Load all tables
tables = load_tables_from_profile('my_profile')

# Load specific tables
tables = load_tables_from_profile('my_profile', ['flights', 'carriers'])

# Use tables
print(list(tables.keys()))
flights = tables['flights']
```

### Create Semantic Models

```python
from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.profile import load_tables_from_profile

# Load tables from profile
tables = load_tables_from_profile('my_duckdb', ['flights_tbl'])

# Create semantic model
flights = to_semantic_table(tables['flights_tbl'], name='flights')
flights = flights.with_measures(
    flight_count=lambda t: t.count(),
    total_distance=lambda t: t.distance.sum(),
)

# Query
result = flights.group_by('carrier').aggregate('flight_count').execute()
```

## Best Practices

### 1. Use Local profile.yml for Projects

**Do this:**
```
my_project/
  ├── profile.yml          # Project-specific profiles
  ├── models.yml           # References profiles
  └── analysis.py
```

```yaml
# profile.yml
dev:
  type: duckdb
  database: dev.db

staging:
  type: duckdb
  database: staging.db
```

### 2. Never Commit Secrets

**Bad:**
```yaml
prod:
  type: postgres
  password: my_secret_password  # ❌ Never do this!
```

**Good:**
```yaml
prod:
  type: postgres
  password: ${POSTGRES_PASSWORD}  # ✅ Use env vars
```

### 3. Use Descriptive Profile Names

**Bad:**
```yaml
db1:
  type: postgres
db2:
  type: postgres
```

**Good:**
```yaml
analytics_dev:
  type: postgres
analytics_prod:
  type: postgres
```

### 4. Document Required Environment Variables

Create a `.env.example` file:

```bash
# .env.example
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mydb
POSTGRES_USER=myuser
POSTGRES_PASSWORD=changeme
```

### 5. Organize Profiles by Environment

```yaml
# profile.yml
dev:
  type: duckdb
  database: dev.db

staging:
  type: postgres
  host: staging.example.com
  database: ${STAGING_DB}
  user: ${STAGING_USER}
  password: ${STAGING_PASSWORD}

prod:
  type: postgres
  host: prod.example.com
  database: ${PROD_DB}
  user: ${PROD_USER}
  password: ${PROD_PASSWORD}
```

## Migration from Catalog

If you're migrating from the old `catalog` system:

**Old (catalog):**
```yaml
catalog:
  type: duckdb
  database: ":memory:"
  tables:
    flights_tbl:
      source: "flights.parquet"

flights:
  table: flights_tbl
  dimensions: {...}
```

**New (profile):**

1. Create `profile.yml`:
```yaml
my_db:
  type: duckdb
  database: ":memory:"
```

2. Update models.yml:
```yaml
profile: my_db

flights:
  table: flights_tbl
  dimensions: {...}
```

3. Load data separately:
```python
from boring_semantic_layer.profile import load_profile
from boring_semantic_layer.yaml import from_yaml

# Load connection
con = load_profile('my_db')

# Load data files
flights_tbl = con.read_parquet('flights.parquet')
con.create_table('flights_tbl', flights_tbl)

# Load semantic models
models = from_yaml('models.yml')
```

## Examples

See the `/examples` directory for complete examples:

- `examples/profile.yml` - Example profile configuration
- `examples/profile_example.py` - Complete Python examples
- `examples/yaml_example_with_profile.yml` - YAML configuration example

## Troubleshooting

### Profile Not Found

**Error:**
```
ProfileError: Failed to load profile 'my_db'
```

**Solution:**
Check the search paths:
1. Is there a `./profile.yml` in your current directory?
2. Is the profile saved in `~/.config/xorq/profiles/`?
3. Create one or the other

### Environment Variable Not Set

**Error:**
```
ProfileError: Environment variable not set: POSTGRES_PASSWORD
```

**Solution:**
```bash
export POSTGRES_PASSWORD=your_password
```

### Backend Not Supported

**Error:**
```
ProfileError: Backend 'postgres' not supported or not installed
```

**Solution:**
Install the required backend:
```bash
pip install 'ibis-framework[postgres]'
```

## Advanced Usage

### Multiple Profile Files

```python
# Development profile
dev_con = load_profile('dev', profile_file='config/dev_profiles.yml')

# Production profile
prod_con = load_profile('prod', profile_file='config/prod_profiles.yml')
```

### Dynamic Profile Selection

```python
import os
from boring_semantic_layer.profile import load_profile

# Select profile based on environment
env = os.getenv('ENVIRONMENT', 'dev')
con = load_profile(env)  # Loads 'dev', 'staging', or 'prod' from profile.yml
```

### Temporary Profiles

```python
# Create temporary profile file
import tempfile
import yaml
from pathlib import Path

profile_config = {
    'temp_db': {
        'type': 'duckdb',
        'database': ':memory:'
    }
}

temp_profile = Path(tempfile.gettempdir()) / 'temp_profile.yml'
temp_profile.write_text(yaml.dump(profile_config))

# Use it
con = load_profile('temp_db', profile_file=temp_profile)
```

## See Also

- [Xorq Profiles Documentation](https://docs.xorq.dev/api_reference/backend_configuration/profiles_api)
- [Ibis Backends](https://ibis-project.org/backends/)
- `examples/profile_example.py` - Complete working example
