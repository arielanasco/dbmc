from logger import setup_log
import mysql.connector
from mysql.connector import errorcode
import sys
from datetime import datetime

class Manage:
    """
        Class for managing,cleaning, and saving data to Database.
        Attributes
    """
    def __init__(self,db_config  : str = 'settings.conf'):
        """
            Initialize DB connection and check DB.conf if valid.
        """
        self.db_config = db_config
        self.logger = setup_log(__name__)
        self.db = self.test_db
        self.cursor = self.db.cursor()
        self.empty_table

    def check_header(self,data):
        """
            Checks the header file if the length is valid.
        """
        if len(data) != 4 :
            self.logger.error(f'Column length is invalid({len(data)})')
            sys.exit()
        self.logger.info(f'Column fields are valid')   

    def check_row(self,data):
        """
            Checks the row data if the length is valid. IF valid, the save_query will be called
        """
        if len(data) != 4 :
            self.logger.error(f'Column length is invalid({len(data)})')
            sys.exit()
        self.save_query(data)

    def save_query(self,data):
        """
            Saves the data to DB
        """
        try:
            sql_add_item = ("INSERT INTO m_item "
                "(item_code, item_name, item_brand, item_promo, created_at) "
                "VALUES (%s, %s, %s, %s ,%s)")
            created_at = datetime.now().strftime("%y%m%d%H%M")
            data_item = (data[0],data[1], data[2], data[3],created_at)
            self.cursor.execute(sql_add_item, data_item)
            self.db.commit()
        except Exception as e:
            self.logger.error(f'Error:{e}')

    @property
    def empty_table(self):
        """
            Truncate the m_item table before the process begin.
        """
        try:
            sql_truncate = ("TRUNCATE TABLE m_item")
            self.cursor.execute(sql_truncate)
            self.logger.info(f'Emptied table success!')
        except Exception as e:
            self.logger.error(f'Error:{e}')

    @property
    def test_db(self):
        try:
            self.db = mysql.connector.connect(option_files=self.db_config)
            self.logger.info("Connected to Database")
            return self.db
        except mysql.connector.Error as err: 
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.logger.error("Database does not exist")
            else:
                self.logger.error(err)
            sys.exit()

    @property
    def close_db(self):
        self.logger.info("Closing Database Connection...")
        self.cursor.close()
        self.db.close()

if __name__ == '__main__':
    dB = Manage()
    dB.test_db
    dB.close_db