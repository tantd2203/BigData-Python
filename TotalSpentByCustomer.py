from mrjob.job import MRJob
class TotalPriceByCusomter(MRJob):
    def mapper(self,_,line):
        (CustomerID,Item,OrderAmount) = line.split(',')
        yield CustomerID,float(OrderAmount)
    
    def reducer(self ,CustomerID,OrderAmount):
   
            yield CustomerID,sum(OrderAmount)


if __name__ == '__main__':
    TotalPriceByCusomter.run()