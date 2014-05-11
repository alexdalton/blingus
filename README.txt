This SQL fuzzer takes as input a file that consists of all the fuzz vectors that
should be used while fuzzing. Provided is the default fuzzvectors file.

This fuzzer can act as the basic framework for any SQL fuzzer. The user simply
must first write their own python sender class which is capable of inputing the
different mutated strings to their desired source. The sender class must have
have two functions the first is the send function which sends the provided
string to the source and a check function which checks whether the previously
sent string resulted in an error indicating that the source sent to may be
vulnerable to SQL injection.
