# MapReduce in Golang

This is a simple implementation of MapReduce in Golang. The code is based on the [MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) by Google.

The starting point of this code was from the MIT 6.5840 distributed systems course. There is a test script which can be run by navigating to the `main` directory and running:

```bash
sudo bash test-mr.sh
```

This will run the test script and output the following passing results:

```bash
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```