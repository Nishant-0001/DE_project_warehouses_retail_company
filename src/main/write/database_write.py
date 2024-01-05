from src.main.utility.logging_config import *

class DatabaseWriter:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def write_dataframe(self,df,table_name):
        try:
            logger.info("inside write_dataframe")
            df.write.jdbc(url=self.url,
                          table=table_name,
                          mode="append",
                          properties=self.properties)
            logger.info(f"Data successfully written into {table_name} table ")
        except Exception as e:
            error_message = f"Error occurred while writing data to {table_name} table: {str(e)}"
            logger.error(error_message)
            raise  #Re-raise the exception after logging
            #return {f"Message: Error occured {e}"}
