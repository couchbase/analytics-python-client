# Couchbase Python Analytics Client
Python client for [Couchbase](https://couchbase.com) Analytics.

Currently Python 3.9 - Python 3.13 is supported.

The Analytics SDK supports static typing.  Currently only [mypy](https://github.com/python/mypy) is supported.  You mileage may vary (YMMV) with the use of other static type checkers (e.g. [pyright](https://github.com/microsoft/pyright)).

# Installing the SDK<a id="installing-the-sdk"></a>

Until a version is available on PyPI, the SDK can be installed via pip with the following command (note the `dev` branch in the url).

Install the SDK via `pip`:
```console
python3 -m pip install git+https://github.com/couchbaselabs/analytics-python-client@dev
```

# Using the SDK<a id="using-the-sdk"></a>

Some more examples are provided in the [examples directory](https://github.com/couchbaselabs/analytics-python-client/tree/dev/examples).

**Connecting and executing a query**
```python
from couchbase_analytics.cluster import Cluster
from couchbase_analytics.credential import Credential
from couchbase_analytics.options import QueryOptions


def main() -> None:
    # Update this to your cluster
    # IMPORTANT:  The appropriate port needs to be specified. The SDK's default ports are 80 (http) and 443 (https).
    #             If attempting to connect to Capella, the correct ports are most likely to be 8095 (http) and 18095 (https).
    #             Capella example: https://cb.2xg3vwszqgqcrsix.cloud.couchbase.com:18095
    endpoint = 'https://--your-instance--'
    username = 'username'
    pw = 'password'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    cluster = Cluster.create_instance(endpoint, cred)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = cluster.execute_query(statement)
    all_rows = res.get_all_rows()
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = cluster.execute_query(statement)
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with positional arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
    res = cluster.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with named arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
    res = cluster.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                          'limit': 10}))
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')


if __name__ == '__main__':
    main()

```

## Using the async API
```python
import asyncio

from acouchbase_analytics.cluster import AsyncCluster
from acouchbase_analytics.credential import Credential
from acouchbase_analytics.options import QueryOptions


async def main() -> None:
    # Update this to your cluster
    # IMPORTANT:  The appropriate port needs to be specified. The SDK's default ports are 80 (http) and 443 (https).
    #             If attempting to connect to Capella, the correct ports are most likely to be 8095 (http) and 18095 (https).
    #             Capella example: https://cb.2xg3vwszqgqcrsix.cloud.couchbase.com:18095
    endpoint = 'https://--your-instance--'
    username = 'username'
    pw = 'password'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    cluster = AsyncCluster.create_instance(endpoint, cred)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = await cluster.execute_query(statement)
    all_rows = await res.get_all_rows()
    # NOTE: all_rows is a list, _do not_ use `async for`
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = await cluster.execute_query(statement)
    async for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with positional arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
    res = await cluster.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    async for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with named arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
    res = await cluster.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                                'limit': 10}))
    async for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

if __name__ == '__main__':
    asyncio.run(main())

```
