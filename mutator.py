import random

new_words = ["AND", "OR", "UNION"]

modes = ["any of the possible_mutations", "use best possible_mutations", "use all possible_mutations"]

class mutator:
    def __init__(self, mode, seed=None):
        """Initialize the mutator with the seed and mode"""

        self.mode = mode
        self.testnum = 0
        # list of delimieters, these also qualify as words to remove/add when mutating
        self.delimiters = ["'", '"', ";", "--", "/*", "#", "`", ",", "=", "(", ")", "+"]
        # list of new words to add when addNewWord mutation is used
        self.new_words = ["AND", "OR", "UNION", "DESC users", "EXEC XP_", "'", '"',
                          ";", "--", "/*", "`", "#", "=", ",", ")"]
        # list of function points to use in best mutations mode
        self.best_mutations = [self.addNWords, self.addNNewWords, self.removeNWords,
                               self.addDelimiter, self.removeDelimiter]
        # list of all mutations to use in all mutations mode
        self.all_mutations = [self.addWord, self.removeWord, self.addNchars,
                              self.removeNchars, self.addNewWord, self.addNWords,
                              self.addNNewWords, self.removeNWords, self.addDelimiter,
                              self.removeDelimiter]
        if (seed is not None):
            random.seed(seed)

    def mutate(self, strToMutate, n=None):
        """Takes in a string and mutates it in some way dependent on the mode"""
        return_str = ""
        if self.mode == 0: # best mutations
            mutation_select = random.randrange(0, len(self.best_mutations))
            return_str = self.best_mutations[mutation_select](strToMutate, n)
        elif self.mode == 1: # all mutations
            mutation_select = random.randrange(0, len(self.all_mutations))
            return_str = self.all_mutations[mutation_select](strToMutate, n)
        elif self.mode == 2: # addWord mutation only
            return_str = self.addWord(strToMutate)
        elif self.mode == 3: # addNchars mutation only
            return_str = self.addNchars(strToMutate, n)
        elif self.mode == 4: # addNewWords mutation only
            return_str = self.addNewWord(strToMutate)
        elif self.mode == 5: # addNWords mutation only
            return_str = self.addNWords(strToMutate, n)
        elif self.mode == 6: # addNNewWords mutation only
            return_str = self.addNNewWords(strToMutate, n)
        elif self.mode == 7: # addDelimiter mutation only
            return_str = self.addDelimiter(strToMutate)
        return return_str

    def addWord(self, string, n=None):
        """Mutates the string by adding a copy of an existing word in the string"""
        possible_words = []
        string_copy = string
        for delim in self.delimiters:
            if string_copy.count(delim) > 0:
                possible_words.append(delim)
                string_copy = string_copy.replace(delim, " ")
        possible_words.extend(string_copy.split())
        if possible_words == []:
            return string
        randomWord = possible_words[random.randrange(0, len(possible_words))]
        randomInsert = possible_words[random.randrange(0, len(possible_words))]
        tuple3 = string.partition(randomInsert)
        return tuple3[0] + tuple3[1] + randomWord + tuple3[2]

    def removeWord(self, string, n=None):
        """Removes an existing word from the string"""
        if string == "":
            return ""
        possible_words = []
        string_copy = string
        for delim in self.delimiters:
            if string_copy.count(delim) > 0:
                possible_words.append(delim)
                string_copy = string_copy.replace(delim, " ")
        possible_words.extend(string_copy.split())
        if possible_words == []:
            return string
        randomWord = possible_words[random.randrange(0, len(possible_words))]
        return string.replace(randomWord, "", 1)

    def addNchars(self, string, n=None):
        """Adds N random characters form the string to random locations in the string"""
        out_string = string
        i = 0
        if n is None:
            n = random.randrange(1, 10)
        while i < n:
            i += 1
            sel_index = random.randrange(0, len(string))
            insert_index = random.randrange(0, len(out_string))
            out_string = out_string[0:insert_index] + string[sel_index] + out_string[insert_index:]
        return out_string

    def removeNchars(self, string, n=None):
        """Removes N random characters from the string"""
        if n > len(string) or string == "":
            return ""
        else:
            if n is None:
                n = random.randrange(1, 10)
            i = 0
            out_string = string
            while i  < n:
                i += 1
                remove_index = random.randrange(0, len(out_string))
                out_string = out_string[0:remove_index] + out_string[remove_index + 1:]
            return out_string

    def addNewWord(self, string, n=None):
        """Adds a word from the new words list to the string in a random location"""
        randIndexes = range(len(self.new_words))
        random.shuffle(randIndexes)
        randWord = ""
        for index in randIndexes:
            if string.count(self.new_words[index]) == 0:
                randWord = self.new_words[index]
                break
        possible_words = []
        string_copy = string
        for delim in self.delimiters:
            if string_copy.count(delim) > 0:
                possible_words.append(delim)
                string_copy = string_copy.replace(delim, " ")
        if possible_words == []:
            return string
        randomInsert = possible_words[random.randrange(0, len(possible_words))]
        tuple3 = string.partition(randomInsert)
        return tuple3[0] + tuple3[1] + randWord + tuple3[2]

    def addNWords(self, string, n=None):
        """Adds N words to the string, calls addWord n times"""
        i = 0
        out_string = string
        if n is None:
            n = random.randrange(1, 10)
        while i < n:
            i += 1
            out_string = self.addWord(out_string)
        return out_string

    def addNNewWords(self, string, n=None):
        """Adds N new words to the string, calls addNewWord n times"""
        i = 0
        out_string = string
        if n is None:
            n = random.randrange(1, 10)
        while i < n:
            i += 1
            out_string = self.addNewWord(out_string)
        return out_string

    def removeNWords(self, string, n=None):
        """Removes N words from the string, calls removeWord n times"""
        if string == "":
            return ""
        i = 0
        out_string = string
        if n is None:
            n = random.randrange(1, 4)
        while i < n:
            i += 1
            out_string = self.removeWord(out_string)
        return out_string

    def addDelimiter(self, string, n=None):
        """Adds a random delimiter from the delimiter list to a random location"""
        randIndexes = range(len(self.delimiters))
        random.shuffle(randIndexes)
        randDelim = ''
        for index in randIndexes:
            if string.count(self.delimiters[index]) == 0:
                randDelim = self.delimiters[index]
                break
        possible_words = []
        string_copy = string
        for delim in self.delimiters:
            if string_copy.count(delim) > 0:
                possible_words.append(delim)
                string_copy = string_copy.replace(delim, " ")
        if possible_words == []:
            return string
        randomInsert = possible_words[random.randrange(0, len(possible_words))]
        tuple3 = string.partition(randomInsert)
        return tuple3[0] + tuple3[1] + randDelim + tuple3[2]

    def removeDelimiter(self, string, n=None):
        """Removes a random delimiter from the string"""
        if string == "":
            return ""
        randIndexes = range(len(self.delimiters))
        random.shuffle(randIndexes)
        randDelim = ''
        for index in randIndexes:
            if string.count(self.delimiters[index]) > 0:
                randDelim = self.delimiters[index]
                break
        return string.replace(randDelim, "", 1)
