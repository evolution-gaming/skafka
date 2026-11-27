# Benchmarks

JMH benchmarks for `skafka`. The module is not part of the aggregate build, so `sbt test` and CI never run it. Run
them by hand:

```sh
sbt benchmark/Jmh/run                                                                   # everything
sbt "benchmark/Jmh/run com.evolutiongaming.skafka.consumer.RecordsConversionBenchmark"  # one suite
sbt "benchmark/Jmh/run -prof gc RecordsConversionBenchmark"                             # allocation too
```

`-f` forks, `-wi`/`-w` warmup iterations and duration, `-i`/`-r` measurement iterations and duration, `-p name=value`
pins a `@Param`, `-rf json -rff out.json` saves results. These override the annotations on the class.

Each suite says in its own Scaladoc what it measures and why.

## Baseline

One measurement of the code as it stands, so a later change has something to be compared against. JMH scores are only
comparable within one machine and one JDK, so reproduce this row before reading anything into a difference:

| | |
| --- | --- |
| Machine | Apple M1 Pro, macOS 27.2.1 |
| JDK | OpenJDK 25.0.5 |
| Scala | 2.13.18 |
| Command | `sbt "benchmark/Jmh/run -f 1 -wi 5 -i 5 -w 2s -r 2s -prof gc .*Benchmark.*"` |

Throughput in ops/s, higher is better. Allocation is `gc.alloc.rate.norm` in bytes per operation, lower is better.

### RecordsConversionBenchmark

One operation converts one poll result of `recordsPerPoll` records.

| Benchmark | recordsPerPoll | Score | Allocation |
| --- | ---: | ---: | ---: |
| `ioRuntime` | 1 | 4 430 552 ± 515 642 | 795 |
| `ioRuntime` | 25 | 404 780 ± 13 655 | 14 043 |
| `ioRuntime` | 500 | 35 803 ± 1 262 | 233 731 |
| `ioRuntime` | 3000 | 6 518 ± 80 | 1 390 780 |
| `tryEffect` | 1 | 10 097 098 ± 128 276 | 704 |
| `tryEffect` | 25 | 423 630 ± 4 176 | 14 248 |
| `tryEffect` | 500 | 37 450 ± 712 | 233 552 |
| `tryEffect` | 3000 | 6 430 ± 67 | 1 390 432 |

Replace this table when the baseline moves. The before and after of a single change belong in the description of the
pull request that makes it.
