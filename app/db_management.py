import sqlite3
import numpy as np

class manager():

    table_list_check = False
    db_name = "database.db"

    def __init__(self, db_name="database.db"):
        self.db_name = db_name
        self.check_tables()

    def update(self,table):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")  # this sets the
            cursor = conn.cursor()
            cursor.execute("UPDATE "+table,)
        pass

    def append(self,data=None):
        table = data.type

        if self.exists(table, data.key, data.value):
            return False

        with sqlite3.connect(self.db_name) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")  # this sets the
            cursor = conn.cursor()
            # print("INSERT INTO "+table+data.fieldsAsTuple(1)+" VALUES "+data.fieldsAsTuple(), data.asDict())
            cursor.execute("INSERT INTO "+table+data.fieldsAsTuple(1)+" VALUES "+data.fieldsAsTuple(), data.asDict())

        return True

    # checks if a record exists in the database
    def exists(self, table, key, value):
        count = np.NaN

        # constructing an appended list of keys in the correct format
        if type(key)==str:
            keys = key+"=:"+key
            key_dict = {key:value}
        elif type(key) == tuple:
            keys =""
            key_dict = {}
            for i in range(len(key)):
                if i >0:
                    keys+=" and "+key[i]+"=:"+key[i]
                else:
                    keys+=key[i]+"=:"+key[i]
                key_dict[key[i]]=value[i]

        with sqlite3.connect(self.db_name) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")  # this sets the
            cursor = conn.cursor()
            # cursor.execute("SELECT COUNT(1) FROM "+table+ " WHERE "+key+" = :"+key,{key:value})
            cursor.execute("SELECT EXISTS(SELECT 1 FROM " + table + " WHERE " + keys + ");", key_dict)
            count = cursor.fetchall()[0]
        return (count[0]!=0)

    # retreive all associated records
    def retreive_records(self, table, key, value):
        records = []
        with sqlite3.connect(self.db_name) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")  # this sets the
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM "+table+ " WHERE "+key+" = :"+key,{key:value})
            records = cursor.fetchall()
        return records

    # returns the column names in a table as a tuple
    def get_columns_names(self,table):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")  # this sets the
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM '+table)
            return next(zip(*cursor.description))


    # # get the countryID for a given country. add the product to the table if it does not already exist
    # def get_countryID(self,country_name):
    #     country_record = self.retreive_records('country','name',country_name)
    #     column_names = self.get_columns_names('country')
    #
    #     if len(country_record)==0:
    #         # a record for the country does not exist in the table for countries. will now create an entry for this country in question
    #         tmp_country = country(country_name)
    #         country_record = self.retreive_records('country', 'name', country_name)[0]
    #         del tmp_country
    #     else:
    #         country_record = country_record[0]
    #
    #     return country_record[ column_names.index('RowID') ] # note that countryID is stored as RowID in the table

    # get the productID for a given product. add the product to the table if it does not exist
    def get_primaryKey_value(self,table, key, value, primaryKey):
        records = self.retreive_records(table,key,value)
        column_names = self.get_columns_names(table)

        if len(records)==0:
            # a record for the country does not exist in the table for countries. will now create an entry for this country in question
            tmp_country = exec(table+"("+value+")")
            record = self.retreive_records(table, key, value)[0]
            del tmp_country
        else:
            record = records[0]

        return record[ column_names.index(primaryKey) ] # note that countryID is stored as RowID in the table

    # check if all tables that are required exist in the database. if any is missing, add them
    def check_tables(self):
        if self.table_list_check == True:
            return

        with sqlite3.connect(self.db_name) as conn:
            conn.execute("PRAGMA foreign_keys = ON;") # this sets the
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            list_of_tables = cursor.fetchall()

            # check to see if tables exists. if it does not exist, then create the table
            if not ('product',) in list_of_tables:
                cursor.execute("""CREATE TABLE product (
                                  name TEXT,
                                  HS_code TEXT,
                                  description TEXT,
                                  PRIMARY KEY (HS_code,description))""")
                print('Created a table for products')

            if not ('country',) in list_of_tables:
                cursor.execute("""CREATE TABLE country (
                                  RowID INTEGER PRIMARY KEY,
                                  name TEXT)""")
                # cursor.execute("""UPDATE SQLITE_SEQUENCE SET seq = 1 WHERE name = country;""")
                print('Created a table for countries')

            if not ('company',) in list_of_tables:
                cursor.execute("""CREATE TABLE company (
                                  RowID INTEGER PRIMARY KEY,
                                  name TEXT,
                                  address TEXT,
                                  POC TEXT,
                                  phone TEXT,
                                  email TEXT,
                                  comments TEXT)""")
                print('Created a table for companies')

            # table for relations between companies, products and countries (buyer's list)
            if not ('buyer',) in list_of_tables:
                cursor.execute("""CREATE TABLE buyer (
                                  CompID INTEGER,
                                  PortID INTEGER,
                                  HS_code TEXT,
                                  description TEXT,
                                  FOREIGN KEY(CompID) REFERENCES company(RowID) ON DELETE CASCADE
                                  FOREIGN KEY(PortID) REFERENCES port(RowID) ON DELETE CASCADE
                                  FOREIGN KEY(HS_code,description) REFERENCES product(HS_code,description) ON DELETE CASCADE,
                                  PRIMARY KEY(CompID, PortID, HS_code))""")
                print('Created a table of buyers and related products')

            # table for relations between companies, products and country (seller's list)
            if not ('seller',) in list_of_tables:
                cursor.execute("""CREATE TABLE seller (
                                  CompID INTEGER,
                                  PortID INTEGER,
                                  HS_code TEXT,
                                  description TEXT,
                                  FOREIGN KEY(CompID) REFERENCES company(RowID) ON DELETE CASCADE
                                  FOREIGN KEY(PortID) REFERENCES port(RowID) ON DELETE CASCADE,
                                  FOREIGN KEY(HS_code,description) REFERENCES product(HS_code,description) ON DELETE CASCADE
                                  PRIMARY KEY(CompID, PortID, HS_code))""")
                print('Created a table of sellers and related product')

            # table of available ports
            if not ('port',) in list_of_tables:
                cursor.execute("""CREATE TABLE port (
                                  RowID INTEGER PRIMARY KEY,
                                  name TEXT,
                                  countryID INTEGER,
                                  FOREIGN KEY(countryID) REFERENCES country(RowId))""")
                print('Created a table of ports')

            if not self.table_list_check:
                self.table_list_check = True

class product(manager):
    def __init__(self, HS_code, name="unknown", description="not provided"):
        super().__init__()
        self.name = name
        self.HS_code = HS_code
        self.description = description.lower()
        self.type = "product"
        self.key = ("HS_code","description")
        self.value = (self.HS_code,self.description)
        super().append(self)

    def asDict(self):
        data = {"HS_code": self.HS_code, "name":self.name, "description":self.description}
        return data

    def fieldsAsTuple(self,mode=0):
        if mode == 0:
            return "(:HS_code, :name, :description)"
        else:
            return "(HS_code, name, description)"

class company(manager):
    def __init__(self, name, address=None, POC=None, phone=None, email="", comments=None):
        super().__init__()
        self.name = name.lower()
        self.address = address
        self.email = email.lower()
        self.comments = comments
        self.POC = POC
        self.phone = phone
        self.type = "company"
        self.key = "name"
        self.value = self.name
        super().append(self)

    def asDict(self):
        data = {"name":self.name,"address":self.address, "email":self.email, "POC": self.POC, "comments":self.comments, "phone":self.phone}
        return data

    def fieldsAsTuple(self,mode=0):
        if mode==0:
            return "(:name, :address, :email, :POC, :comments, :phone)"
        else:
            return "(name, address, email, POC, comments, phone)"

class country(manager):
    name = "unassigned"

    def __init__(self, name):
        super().__init__()
        self.name = name.lower()
        self.type = "country"
        self.key = "name"
        self.value = self.name
        super().append(self)

    def asDict(self):
        data = {"name":self.name}
        return data

    def fieldsAsTuple(self,mode =0):
        if mode == 0:
            return "(NULL,:name)"
        else:
            return "(RowID,name)"

class port(manager):
    def __init__(self, name, country=None):
        super().__init__()
        self.name = name.lower()
        self.type = "port"
        self.key = "name"
        self.value = self.name

        if type(country) == str:
            self.country = country.lower()
            self.countryID = super().get_primaryKey_value('country', 'name', self.country, 'RowID')
        else:
            self.countryID = None

        super().append(self)

    def asDict(self):
        data = {"name": self.name, "countryID": self.countryID}
        return data

    def fieldsAsTuple(self, mode=0):
        if mode == 0:
            return "(:name,:countryID)"
        else:
            return "(name,countryID)"

class order(manager):
    def __init__(self,buyer,seller,HS_code, export_port,import_port,description=None):
        # going to assume here that ports, product with appropriate HS_code is available in the database
        super().__init__()
        self.buyer_name = buyer.lower()
        self.seller_name = seller.lower()
        self.product = HS_code
        self.export_port = export_port.lower()
        self.import_port = import_port.lower()
        self.buyerID = super().get_primaryKey_value('company','name',self.buyer_name,'RowID')
        self.sellerID = super().get_primaryKey_value('company', 'name', self.seller_name, 'RowID')
        self.export_port_ID = super().get_primaryKey_value('port','name',self.export_port,'RowID')
        self.import_port_ID = super().get_primaryKey_value('port', 'name', self.import_port, 'RowID')
        self.description = description

        # updating the buyer's list
        self.type = "buyer"
        self.key = ("CompID","PortID","HS_code")
        self.value = (self.buyerID, self.import_port_ID,self.product)
        self.portID = self.import_port_ID
        self.compID = self.buyerID
        super().append(self)

        # updating the seller's list
        self.type = "seller"
        self.key = ("CompID", "PortID", "HS_code")
        self.value = (self.sellerID, self.export_port_ID, self.product)
        self.portID = self.export_port_ID
        self.compID = self.sellerID
        super().append(self)

    def asDict(self):
        data = {"CompID": self.compID, "PortID": self.portID, "HS_code":self.product, "description": self.description}
        return data

    def fieldsAsTuple(self, mode=0):
        if mode == 0:
            return "(:CompID,:PortID, :HS_code, :description)"
        else:
            return "(CompID,PortID,HS_code, description)"

if __name__=="__main__":
    new_manager = manager()
    new_country = country('usa')
    product(1234,name='nyquil',description='addictive drug that is used by people who are struggling from mid-life crisis')
    # print(new_manager.exists('product',('name','HS_code'),('nyquil',1234)))
    port('houston','usa')
    port('new york','usa')
    company('Daichem')
    company('Ranson')
    company('Microsoft')
    product(1235)
    order(buyer='daichem',seller='microsoft',HS_code=1234,export_port='houston',import_port='new york')