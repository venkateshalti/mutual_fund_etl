from xml.etree import ElementTree as ET
import pandas as pd
pd.set_option('display.max_columns', 15)
from datetime import datetime

class ParserXML():
    fund_sales_list = list()
    def __init__(self):
        print("parser object created")

    def parseXML( self, FILE, INPUT_PATH ):
        """ parse XML to extract data """

        self.FILE = FILE
        self.INPUT_PATH = INPUT_PATH

        FILE_PARSER = INPUT_PATH + FILE

        xtree = ET.parse( FILE_PARSER )
        xroot = xtree.getroot()

        """ Definition of the functions  """
        def getValue(value):
            """ This function returns the value of the XML object
            in case it is not None """
            if (value is not None):
                return value.text
            else:
                return None

        def funcion_list_fund_sales( self, xroot ):
            """ saves in a list all customer details under 'customer' tag"""

            self.fund_sales_list.clear()
            for node in xroot.findall('Customer'):
                customer_id = node.attrib.get('id')
                firstname = node.find('FirstName')
                lastname = node.find('LastName')
                contact_no = node.find('ContactNo')
                email_id = node.find('Email')
                policy_type = node.find('policy-type')
                policy_name = node.find('policy-name')
                regular_direct = node.find('regular-direct')
                unit_value = node.find('Unit-Value')
                units = node.find('Units')
                value = node.find('Value')
                purchase_date = node.find('Purchase-Date')


                current_iteration = 1
                address = []
                for node_2 in node.findall('Address'):
                    address_id = node_2.attrib.get('id')
                    city = node_2.find('City')
                    state = node_2.find('State')
                    zip = node_2.find('Zip')
                    address.append(city.text+', '+state.text+', '+zip.text)

                try:
                    address[1]
                except IndexError:
                    address.append("")

                self.fund_sales_list.append({"customer_id": customer_id
                                                  , "firstname" : getValue(firstname)
                                                  , "lastname" : getValue(lastname)
                                                  , "contact_no" : getValue(contact_no)
                                                  , "email_id" : getValue(email_id)
                                                  , "primary_address": address[0]
                                                  , "secondary_address": address[1]
                                                  , "policy_type": getValue(policy_type)
                                                  , "policy_name": getValue(policy_name)
                                                  , "regular_direct": getValue(regular_direct)
                                                  , "unit_value": getValue(unit_value)
                                                  , "units": getValue(units)
                                                  , "value": getValue(value)
                                                  , "purchase_date": getValue(purchase_date)
                                            })


        """ Parse the XML file to get the values of the lists """
        funcion_list_fund_sales(self, xroot )

        """ Create the dataframes from the dictionary lists, it works better with Pandas """
        #self.fund_sales_df = pd.DataFrame( self.fund_sales_list )
        #print(self.fund_sales_df)
        return self.fund_sales_list

