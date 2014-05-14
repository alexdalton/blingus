import time
import importlib
from mutator import mutator

class fuzzer:
    def __init__(self, interface, filename, seed=None, mode=0):
        # Import sender class and initialize
        self.interfaceClass = importlib.import_module(interface)
        self.interface = self.interfaceClass.interface()
        self.mutator = mutator(mode, seed)

        # Get fuzz vectors
        fuzzFile = open(filename)
        self.fuzz_vectors = []
        for line in fuzzFile:
            if line != "\n":
                self.fuzz_vectors.append(line.rstrip("\n"))
        fuzzFile.close()

    def send(self, message):
        print message
        #self.interface.send(message)

    def fuzz(self, runTime=10):
        startTime = time.time()
        message = ""
        i = 0
        j = 0
        while((startTime + runTime) > time.time()):
            if i < len(self.fuzz_vectors):
                message = self.fuzz_vectors[i]
                self.send(message)
                i += 1
            else:
                # Mutate the vector 3 times and send after each mutations
                message = self.mutator.mutate(self.fuzz_vectors[j])
                self.send(message)
                message = self.mutator.mutate(self.fuzz_vectors[j])
                self.send(message)
                message = self.mutator.mutate(self.fuzz_vectors[j])
                self.send(message)
                j = (j + 1) % len(self.fuzz_vectors)
