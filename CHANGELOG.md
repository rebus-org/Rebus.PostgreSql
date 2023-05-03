# Changelog

## 2.0.0-a1
* Test release

## 2.0.0-b01
* Test release

## 2.0.0
* Release 2.0.0

## 2.1.0
* Add PostgreSQL transport implementation - thanks [jmkelly]

## 2.2.0
* Add one-way configuration extension for the transport - thanks [jmkelly]
* Fix nuspec

## 3.0.0
* Update to Rebus 3

## 4.0.0
* Update to Rebus 4
* Add .NET Core support
* Add ability to customize `NpgsqlConnection` before it is used (e.g. to provide a certificate validation callback) - thanks [enriquein]

## 4.1.0
* Add async bottleneck for outgoing messages to avoid concurrency issues accessing a shared connection - thanks [dtabuenc]

## 5.0.0
* Make connection provider configurable - thanks [dtabuenc]

## 6.0.0
* Change ordering such that priority is reversed (i.e. higher priorities are preferred) and such that visible time takes precedence over insertion order, meaning that deferred messages are ordered more naturally
* Rename misleading parameter
* Update Npgsql dependency to 4.1.3 and System.Data.SqlClient to 4.8.0 to get the latest security fixes
* Update Rebus dependency to v. 5
* Enable Postgres connection to enlist in ambient transaction - thanks [KasperDamgaard]

## 7.0.0
* Update to Rebus 6

## 7.1.0
* Delete expired messages regardless of their destination queue, thus making it possible for abandoned messages to expire - thanks [zabulus]

## 7.1.1
* Add index to improve dequeueing performance for the transport - thanks [knutsr]

## 7.2.0
* Add target for .NET 5 - thanks [mastersign]

## 7.3.0
* Add flag in DB provider to indicate that the connection/transaction is managed externally - thanks [Laurianti]

## 7.3.1
* Additional flag for indicating that connection/transaction is managed externally - thanks [Laurianti]

## 7.4.0
* Optional parameters to enable configuring the expired messages cleanup interval - thanks [Laurianti]

## 8.0.0
* Remove unnecessary System.Data.SqlClient dependency
* Update Npgsql dependency to 6.0.4

## 8.0.1
* Fix bug where `isCentralized` was not actually used - thanks [mts44]

## 8.1.0
* Make saga data serializer configurable - thanks [mmdevterm]

## 8.2.0-b3
* Add outbox - thanks [matt-psaltis]
* Add schema support - thanks [patrick11994]
* Add ambient transaction support for outbox and fix bug in outbox storage - thanks [jwoots]

## 9.0.0-alpha04
* Update to Rebus 8
* Clean up outbox messages as they're processed - thanks [jwoots] 
* Expose optional `schemaName` parameter from underlying configuration method
* Use now() instead of clock_timestamp() to allow better index on cleanup deletes - thanks [jmkelly]

---

[dtabuenc]: https://github.com/dtabuenc
[enriquein]: https://github.com/enriquein
[jmkelly]: https://github.com/jmkelly
[jwoots]: https://github.com/jwoots
[KasperDamgaard]: https://github.com/KasperDamgaard
[knutsr]: https://github.com/knutsr
[Laurianti]: https://github.com/Laurianti
[mastersign]: https://github.com/mastersign
[matt-psaltis]: https://github.com/matt-psaltis
[mmdevterm]: https://github.com/mmdevterm
[mts44]: https://github.com/mts44
[patrick11994]: https://github.com/patrick11994
[zabulus]: https://github.com/zabulus
