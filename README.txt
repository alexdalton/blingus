This SQL fuzzer takes as input a file that consists of all the fuzz vectors that
should be used while fuzzing. Provided is the default fuzzvectors file.

This fuzzer can act as the basic framework for any SQL fuzzer. The user simply
must first write their own python interface class which is capable of inputing the
different mutated strings to the desired source. The interface class must have
have two functions the first is the send function which sends the provided
string to the source which then returns a boolean if what's returned indicates
a SQL injection vulnerability and an init function which does any logging in
and setup that is required before mutated fuzz vectors should be sent.
