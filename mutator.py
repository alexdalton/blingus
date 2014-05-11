import random

possible_mutations = ["addword", "removeWord", "addNchars", "removeNchars",
                      "addDelimiter", "removeDelimiter", "randomNBytes", "combineVectors",
                     ]
delimiters = ["'", '"', ";", "--", "/*", "#"]

class mutator:
    def __init__(self, mode, initial_str, seed=None):
        self.mode = mode
        self.initial_str = initial_str
        self.testnum = 0
        if (seed is not None):
            random.seed(seed)

    def mutate(self, strToMutate):
        return strToMutate + possible_mutations[random.randrange(0, len(possible_mutations) - 1)]

    def get_fuzzvectors(self, filename):
        fuzzFile = open(filename)
        fuzz_vectors = []
        for line in fuzzFile:
            if line != "\n":
                fuzz_vectors.append(line.rstrip("\n"))
        fuzzFile.close()
        return fuzz_vectors
