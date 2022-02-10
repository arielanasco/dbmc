from logger import setup_log
import mysql.connector
from mysql.connector import errorcode
import sys
import os
import configparser

class Manage:
    """
        Class for managing,cleaning, and saving data to Database.
        Attributes
    """
    def __init__(self,db_config  : str = 'settings.conf'):
        """
            Initialize DB connection and check DB.conf if valid.
        """
        self.logger = setup_log(__name__)

        if os.path.exists(db_config):
            self.db_config = db_config
        else:
            config = configparser.ConfigParser()
            config['client'] = {
                                'database': '<DATABASE_NAME_HERE>',
                                'HOST': '<HOST_NAME_HERE>',
                                'PORT': '<PORT_HERE>',
                                'USER': '<USERNAME_HERE>',
                                'PASSWORD': '<PASSWORD_HERE>',
                                'default-character-set': 'utf8',
                                }
            with open('settings.conf', 'w') as configfile:
                config.write(configfile)
            self.logger.info(f"Cannot found {db_config}. Created the settings.conf. Please insert the necessary credentials before connecting to DB. ")
            sys.exit()

        try:
            self.db = mysql.connector.connect(option_files=self.db_config)
            if self.db.is_connected():
                self.logger.info(f"Connected to MySQL Server version({self.db.get_server_info()})")
        except mysql.connector.Error as err: 
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.logger.info("ERROR USERNAME/PASSWORD")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.logger.info("ERROR DATABASE")
            else:
                self.logger.info(err)
            sys.exit()

        if isinstance(self.db,mysql.connector.connection.MySQLConnection):
            self.cursor = self.db.cursor()
        else:
            sys.exit()

    def push(self,table_name, columns = [], data = []):
        """
            Saves the data to DB
        """
        try:
            sql_add_item = (f"INSERT INTO {table_name} "
                f"""{str(tuple(col for col in columns)).replace("'","")}"""
                f"""VALUES {str(tuple('%s' for _ in columns)).replace("'","")}""")
            self.cursor.execute(sql_add_item, data)
            self.db.commit()
        except Exception as e:
            self.logger.info(f'Error:{e}')

    def get(self,query):
        """
            Get data from tables
        """
        table_name = query["table"]
        columns    = query["field"]
        filter     = query["filter"]
        try:
            sql_get_items =(f"""SELECT {",".join([col for col in columns])}"""
            f""" FROM {table_name}"""
            )
            if filter:
                sql_get_items += f""" WHERE {filter}"""
            self.cursor.execute(sql_get_items)
            records = self.cursor.fetchall()
            return records
        except Exception as e:
            self.logger.error(f"Error {e}")

    @property
    def close_db(self):
        try:
            self.logger.info("Closing connection")
            self.db.close()
            self.cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Closing connection error {e}")
            return False

if __name__ == '__main__':
    manager1 = Manage()

    query = {
        "table"  : "m_city",
        "field"  : ["city_nm","city_cd"],
        "filter" : "city_cd = 473014"
    }
    
    manager1.db.is_connected()
    qw =manager1.get(query)
    print(manager1.close_db)