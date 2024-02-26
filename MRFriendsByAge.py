from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    
    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)            
     

    # #  count  average  with age by key and numFreinds by value
    def reducer(self , age ,numFrineds ):g
            #  total = 0
            #  numElement =0

            #  for x in numFrineds:
            #     total +=x
            #     numElement+=1
                
            #  average =  total / numElement 
             v=  numFrineds
        
             yield age , list(v)

    


if __name__ == '__main__':
    MRFriendsByAge.run()