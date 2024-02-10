
from mrjob.job import MRJob
class WordFrequency(MRJob):

    
    def mapper (self,_,line):
        words = line.split()

        for word in words:
            yield word.lower(), 1
    def reducer(self , key, vaule ):
       yield key, sum(vaule)
       
if __name__ == '__main__':
    WordFrequency.run()
