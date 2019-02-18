# AeroUpdateTest

Simple piece of code that:
1. on launch tries to do 10'000 inserts with PK 1-10000 (random bin values).
2. after that, in an endless loop just updates those 10k records with new bin values (random values all the way).
3. It can print EVERYTHING, or just ERRORS and summaries, or it can print just summaries.
4. every time it does it's 10k update loop, it prints the one-line summary.

It has on first run set INSERT_ONLY write policy and UPDATE_ONLY policy on subsequent runs.

```
Usage:
./updater NodeIP NodePORT NamespaceName setName binName binValueLength debugLevel(0=JustSummaries,1=LogErrors,2=debug) [username] [password]

Example:
./AeroUpdateOnly 127.0.0.1 3000 bar testSET testBin 20 1
```

Supports user:pass or no-auth. Does not support TLS.
