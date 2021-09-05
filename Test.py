from mrjob.job import MRJob
from mrjob.step import MRStep
import re

REGEX_ONLY_WORDS = "[\w']+"
MAIN_CHARACTER_FIRST_NAME = "sherlock"
MAIN_CHARACTER_LAST_NAME = "holmes"


class MRDataMining(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words, reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_make_counts_key, reducer = self.reducer_output_words)
        ]

    def mapper_get_words(self, _, line):
        FLAG = 0
        words = re.findall(REGEX_ONLY_WORDS, line)
        for word in words:
            is_Main_Character = (word.lower() == MAIN_CHARACTER_FIRST_NAME or word.lower() == MAIN_CHARACTER_LAST_NAME)
            if(is_Main_Character and FLAG == 0):
                FLAG = 1
                yield "Main character", 1
            elif(FLAG):
                FLAG = 0
                if(not is_Main_Character):
                    yield word.lower(), 1
            else:
                yield word.lower(), 1
                    

    def reducer_count_words(self, word, values):
        yield word, sum(values)

    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word

    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRDataMining.run()