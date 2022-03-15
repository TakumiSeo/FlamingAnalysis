import mysql.connector

class DelDB:
    def __init__(self, mysql_info):
        self.mydb = mysql.connector.connect(host=mysql_info['DB_HOST'], port=3306,
                                            db=mysql_info['DB_NAME'], user=mysql_info['DB_USER'],
                                            passwd=mysql_info['DB_PASSWORD'], charset=mysql_info['CHARSET'])
        self.cursor = self.mydb.cursor()

    def del_sql(self, table, day_id, is_commit=False):
        sql_out = "DELETE FROM %s WHERE day_id = '%s'" % (table, day_id);
        self.cursor.execute(sql_out)
        self.mydb.commit()
        return True

    def all_close(self):
        self.mydb.close()
        self.cursor.close()
