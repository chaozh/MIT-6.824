#!/bin/bash
#
# Script for running `go test` a bunch of times, in parallel, storing the test
# output as you go, and showing a nice status output telling you how you're
# doing.
#
# Normally, you should be able to execute this script with
#
#   ./go-test-many.sh
#
# and it should do The Right Thing(tm) by default. However, it does take some
# arguments so that you can tweak it for your testing setup. To understand
# them, we should first go quickly through what exactly this script does.
#
# First, it compiles your Go program (using go test -c) to ensure that all the
# tests are run on the same codebase, and to speed up the testing. Then, it
# runs the tester some number of times. It will run some number of testers in
# parallel, and when that number of running testers has been reached, it will
# wait for the oldest one it spawned to finish before spawning another. The
# output from each test i is stored in test-$i.log and test-$i.err (STDOUT and
# STDERR respectively).
#
# The options you can specify on the command line are:
#
#   1) how many times to run the tester (defaults to 100)
#   2) how many testers to run in parallel (defaults to the number of CPUs)
#   3) which subset of the tests to run (default to all tests)
#
# 3) is simply a regex that is passed to the tester under -test.run; any tests
# matching the regex will be run.
#
# The script is smart enough to clean up after itself if you kill it
# (in-progress tests are killed, their output is discarded, and no failure
# message is printed), and will automatically continue from where it left off
# if you kill it and then start it again.
#
# By now, you know everything that happens below.
# If you still want to read the code, go ahead.

if [ $# -eq 1 ] && [ "$1" = "--help" ]; then
	echo "Usage: $0 [RUNS=100] [PARALLELISM=#cpus] [TESTPATTERN='']"
	exit 1
fi

# If the tests don't even build, don't bother. Also, this gives us a static
# tester binary for higher performance and higher reproducability.
if ! go test -c -o tester; then
	echo -e "\e[1;31mERROR: Build failed\e[0m"
	exit 1
fi

# Default to 100 runs unless otherwise specified
runs=100
if [ $# -gt 0 ]; then
	runs="$1"
fi

# Default to one tester per CPU unless otherwise specified
parallelism=$(grep -c processor /proc/cpuinfo)
if [ $# -gt 1 ]; then
	parallelism="$2"
fi

# Default to no test filtering unless otherwise specified
test=""
if [ $# -gt 2 ]; then
	test="$3"
fi

# Figure out where we left off
logs=$(find . -maxdepth 1 -name 'test-*.log' -type f -printf '.' | wc -c)
success=$(grep -E '^PASS$' test-*.log | wc -l)
((failed = logs - success))

# Finish checks the exit status of the tester with the given PID, updates the
# success/failed counters appropriately, and prints a pretty message.
finish() {
	if ! wait "$1"; then
		if command -v notify-send >/dev/null 2>&1 &&((failed == 0)); then
			notify-send -i weather-storm "Tests started failing" \
				"$(pwd)\n$(grep FAIL: -- *.log | sed -e 's/.*FAIL: / - /' -e 's/ (.*)//' | sort -u)"
		fi
		((failed += 1))
	else
		((success += 1))
	fi

	if [ "$failed" -eq 0 ]; then
		printf "\e[1;32m";
	else
		printf "\e[1;31m";
	fi

	printf "Done %03d/%d; %d ok, %d failed\n\e[0m" \
		$((success+failed)) \
		"$runs" \
		"$success" \
		"$failed"
}

waits=() # which tester PIDs are we waiting on?
is=()    # and which iteration does each one correspond to?

# Cleanup is called when the process is killed.
# It kills any remaining tests and removes their output files before exiting.
cleanup() {
	for pid in "${waits[@]}"; do
		kill "$pid"
		wait "$pid"
		rm -rf "test-${is[0]}.err" "test-${is[0]}.log"
		is=("${is[@]:1}")
	done
	exit 0
}
trap cleanup SIGHUP SIGINT SIGTERM

# Run remaining iterations (we may already have run some)
for i in $(seq "$((success+failed+1))" "$runs"); do
	# If we have already spawned the max # of testers, wait for one to
	# finish. We'll wait for the oldest one beause it's easy.
	if [[ ${#waits[@]} -eq "$parallelism" ]]; then
		finish "${waits[0]}"
		waits=("${waits[@]:1}") # this funky syntax removes the first
		is=("${is[@]:1}")       # element from the array
	fi

	# Store this tester's iteration index
	# It's important that this happens before appending to waits(),
	# otherwise we could get an out-of-bounds in cleanup()
	is=("${is[@]}" $i)

	# Run the tester, passing -test.run if necessary
	if [[ -z "$test" ]]; then
		./tester -test.v 2> "test-${i}.log" > "test-${i}.log" &
		pid=$!
	else
		./tester -test.run "$test" -test.v 2> "test-${i}.log" > "test-${i}.log" &
		pid=$!
	fi

	# Remember the tester's PID so we can wait on it later
	waits=("${waits[@]}" $pid)
done

# Wait for remaining testers
for pid in "${waits[@]}"; do
	finish "$pid"
done

if ((failed>0)); then
	exit 1
fi
exit 0
