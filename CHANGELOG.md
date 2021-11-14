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

---

[dtabuenc]: https://github.com/dtabuenc
[enriquein]: https://github.com/enriquein
[jmkelly]: https://github.com/jmkelly
[KasperDamgaard]: https://github.com/KasperDamgaard
[knutsr]: https://github.com/knutsr
[Laurianti]: https://github.com/Laurianti
[mastersign]: https://github.com/mastersign
[zabulus]: https://github.com/zabulus
