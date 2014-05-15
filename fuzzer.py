import time
import importlib
from mutator import mutator

class fuzzer:
    def __init__(self, interface, filename, seed=None, mode=0):
        """
        Initializes the fuzzer class wit the interface to use and file to import
        the fuzz vectors from, also passes seed and mode to the mutator
        """
        # Import sender class and initialize
        self.interfaceClass = importlib.import_module(interface)
        self.interface = self.interfaceClass.interface()
        self.mutator = mutator(mode, seed)
        self.interfaceName = str(interface)

        # Get fuzz vectors
        fuzzFile = open(filename)
        self.fuzz_vectors = []
        for line in fuzzFile:
            if line != "\n" and line[0] != "#":
                self.fuzz_vectors.append(line.rstrip("\n"))
        fuzzFile.close()

    def send(self, message):
        """Uses the interface class to send the SQL string to the interface"""
        return self.interface.send(message)

    def fuzz(self, runTime=10):
        """Main fuzzing function that fuzzes an interface for time = runTime"""
        startTime = time.time()
        message = ""
        i = 0
        j = 0
        outfile = open("logs/log." + self.interfaceName + "." + str(startTime), "w")
        while((startTime + runTime) > time.time()):
            if i < len(self.fuzz_vectors):
                message = self.fuzz_vectors[i]
                success = self.send(message)
                outfile.write(message + " - " + str(success) + "\n")
                i += 1
            else:
                # Mutate the vector 3 times and send after each mutations
                i = 0
                while i < 3:
                    message = self.mutator.mutate(self.fuzz_vectors[j])
                    success = self.send(message)
                    outfile.write(message + " - " + str(success) + "\n")
                    i += 1

                outfile.write(message + " - " + str(success) + "\n")
                j = (j + 1) % len(self.fuzz_vectors)
        outfile.close()
