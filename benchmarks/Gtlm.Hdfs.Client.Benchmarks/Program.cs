using BenchmarkDotNet.Running;
using Gtlm.Hdfs.Client.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(ChecksumBenchmarks).Assembly).Run(args);
