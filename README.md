# Rebus.PostgreSql

[![install from nuget](https://img.shields.io/nuget/v/Rebus.PostgreSql.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.PostgreSql)

Provides a PostgreSQL-based persistence for [Rebus](https://github.com/rebus-org/Rebus) for

* sagas
* subscriptions
* timeouts
* transport

Note:  In your npgsql connection string, if you are using the default settings, set your maximum pool size=30 to avoid connection pool starvation issues.

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---


