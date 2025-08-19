import sys
from networksecurity.logging import logger
# inheriting from Python’s built-in Exception class
class NetworkSecurityException(Exception):
    def __init__(self,error_message,error_details:sys):  #error_details is initialized at the time of calling the constructor with sys
        self.error_message = error_message
        _,_,exc_tb = error_details.exc_info()
        
        self.lineno=exc_tb.tb_lineno
        self.file_name=exc_tb.tb_frame.f_code.co_filename 
    
    def __str__(self):
        return "Error occured in python script name [{0}] line number [{1}] error message [{2}]".format(
        self.file_name, self.lineno, str(self.error_message))
        
if __name__=='__main__':
    try:
        logger.logging.info("Enter the try block")
        a=1/0
        print("This will not be printed",a)  
    except Exception as e: #Exception is a base class for all built-in exceptions
           raise NetworkSecurityException(e,sys) #print(exception_object)  # Automatically calls exception_object.__str__()