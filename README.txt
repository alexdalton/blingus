This SQL fuzzer takes as input a file that consists of all the fuzz vectors that
should be used while fuzzing. Provided is the default fuzzvectors file.

This fuzzer can act as the basic framework for any SQL fuzzer. The user simply
must first write their own python interface class which is capable of inputing
the different mutated strings to the desired source. The interface class must
have two functions the first is the send function which sends the provided
string to the source which then returns a boolean if what's returned indicates
a SQL injection vulnerability and an init function which does any logging in
and setup that is required before mutated fuzz vectors should be sent.

The fuzzer operates in several different modes.
The modes are as follows:
0 - uses a set of best mutations to generate permutations of the fuzz vectors
1 - uses entire set of mutations to generate permutations of the fuzz vectors
2 - uses only the add word mutation, where a word is any word or delimiter
    currently in the fuzz vector
3 - uses only the add N characters mutations, n of the current characters in
    the fuzz vector are added randomly
4 - uses only the add new words mutation, where a new word is any word that
    appears in the new words list but not in the fuzz vector
5 - uses only the add N words mutation, calls add word mutation N times
6 - uses only the add N new words mutation, calls add new word mutation N times
7 - uses only the add delimiter mutation, randomly adds an entry from the
    delimiter list
