import os  # use walk method to walk through each filename, and path to check if path exists
import urllib.request  # this is to make URL call to get RAR file
from pyunpack import Archive  # to unrar
#  pyunpack is dependent on patool, you can check in it's __init__, so please install patool via pip
class FileManagement():

    DOWNLOADED_FILE_NAME = "offline_external_sales.rar"  # this name will be given to the downloaded rar file
    # the original file is a zip file, but we are saving it as a rar file
    xml_files_list = list()  # this empty list wil eventually be filled with location path of extracted XMLs

    def __init__( self):
        print("FileManagement initialized")

    def create_output_path_if_not_exists(self, LOCATION):  # checks if the location exists
        if ( not os.path.exists( LOCATION ) ):
            os.makedirs( LOCATION )  # create location if it doesn't exist

    # downloading rar file from URL and storing to location from config file
    def downloadRequestRarURL(self, REQUEST_DOWNLOAD_URL, DOWNLOAD_LOCATION ):
        print('Beginning file download with urllib2...')

        self.create_output_path_if_not_exists(DOWNLOAD_LOCATION)  # call to create location if needed
        # if it is rest service, you can include api_key in the URL
        urllib.request.urlretrieve( REQUEST_DOWNLOAD_URL, DOWNLOAD_LOCATION + self.DOWNLOADED_FILE_NAME )

    # unrar downloaded file from location and store all XMLs to location from config file
    def unRarDownloadedFile(self, DOWNLOAD_LOCATION, DESTINATION_LOCATION ):

        self.create_output_path_if_not_exists(DESTINATION_LOCATION)  # call to create location if needed

        Archive( DOWNLOAD_LOCATION + self.DOWNLOADED_FILE_NAME ).extractall( DESTINATION_LOCATION )
        # archive class takes location+file name as input
        # it has an extractall method that takes destination location to dump all XMLs

    def getFilesXMLFromOrigin( self, LOCATION ):
        """ This function saves in the received list, the location of all the
        files that are in the PATH that it receives by parameter """
        self.xml_files_list.clear()
        dictionary = None
        self.xml_files_list = list()
        for root, dirs, files in os.walk( LOCATION ):
            print("root:", root, "dirs:", dirs, "files:", files)
            for file in files:
                print("file:", file)
                if file.endswith(".xml") | file.endswith(".XML"):

                    dictionary = { "INPUT_PATH": root
                                    , "FILE": file }

                    self.xml_files_list.append(dictionary)

        #print(self.xml_files_list)  # this is a list of dictionaries
        return self.xml_files_list