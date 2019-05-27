import pandas as pd
import numpy as np
from datetime import datetime
import os, glob, sys, subprocess, hashlib, shutil, zipfile, tarfile
from xml.etree import ElementTree
from xml.dom import minidom
from xml.etree.ElementTree import Element, XML  # SubElement, Comment, tostring, XMLParser

zip_file_name = "zipFile.zip" # this name is same as master csv filename

base_Dir = os.getcwd()
input_Dir = base_Dir + os.sep + "input" + os.sep  # it contains Master CSV files
output_Dir = base_Dir + os.sep + "output" + os.sep  # it contains Baby CSV along with XML files
log_Dir = output_Dir + "log_Dir" + os.sep
pre_dateFD_struct = "ILS_Collection"+os.sep+"Data"+os.sep+"Calibrated" +os.sep

# Start of Initial setup
if not os.path.exists(input_Dir):
    print("input folder is not present Under: " + base_Dir)
    os.mkdir(input_Dir)
    print("input folder created: " + input_Dir + " Place Master CSV files and Re-Run the script to process")
    print("Program terminated..!!!")
    sys.exit(0)

if len(glob.glob(input_Dir + "*.csv")) == 0:
    print("No input files to process")
    print("Program terminated..!!!")
    sys.exit(0)

if not os.path.exists(output_Dir):
    print("Creating output folders: " + output_Dir) #+pre_dateFD_struct)
    os.makedirs(output_Dir)
    os.makedirs(log_Dir) # Todo correct the code
elif not os.path.exists(log_Dir):
    os.makedirs(log_Dir)
# End of Initial setup

info_model_version = "1.9.0.0"
req_col_positions = [0, 2, 13, 14, 16, 18, 21, 23, 26, 28]
req_col_names = ['Frame Number', 'UTC Time', 'Temperature (deg C)', 'TCM Id',
                 'Fine Sensor (Z)', 'Coarse Sensor (Z)', 'Fine Sensor (Y)',
                 'Coarse Sensor (Y)', 'Fine Sensor (X)', 'Coarse Sensor (X)']
units = ["NA", "ms", "degC", "NA", "m/s**2", "m/s**2", "m/s**2", "m/s**2", "m/s**2", "m/s**2", "ms"]
col_discription_dict = {'Coarse Sensor (X)': 'Coarse sensor data for X-axis',
                        'Coarse Sensor (Y)': 'Coarse sensor data for Y-axis',
                        'Coarse Sensor (Z)': 'Coarse sensor data for Z-axis',
                        'Fine Sensor (X)': 'Fine sensor data for X-axis',
                        'Fine Sensor (Y)': 'Fine sensor data for Y-axis',
                        'Fine Sensor (Z)': 'Fine sensor data for Z-axis',
                        'Frame Number': 'Each frame number corresponds to 5ms of seismic data for both coarse and fine sensor along X,Y and Z axes',
                        'OBT': 'This is OBT',
                        'TCM Id': 'Temperature Channel Monitoring ID',
                        'Temperature (deg C)': 'Operating Temperature along X,Y andZ-axis',
                        'UTC Time': 'UTC Time'}

# data_types = ["ASCII_Integer", "ASCII_Date_Time_YMD_UTC", "ASCII_Real", "ASCII_Integer", "ASCII_Real", "ASCII_Real",
#               "ASCII_Real", "ASCII_Real", "ASCII_Real", "ASCII_Real", "ASCII_Real"]

data_types = []
type_mapper = {'object': 'ASCII_String','datetime64[ns]': 'ASCII_Date_Time_YMD_UTC'}
type_mapper.update(dict.fromkeys(['float','float32','float64',], 'ASCII_Real'))
type_mapper.update(dict.fromkeys(['int','int32','int64',], 'ASCII_Integer'))

def get_PDS_Types(cols):  #it is dependent on req_col_names
    """ It will return PDS4 data type corresponding builtin data type
    Parameter:
        cols = pandas dtypes i.e. cols.dtypes
    """
    pds_types = []
    for col in cols.values:
        pds_types.append(type_mapper[str(col)])
    return pds_types

def createDateFolder(masterCSV, d):
    dd = d.split('-')
    fs = output_Dir + 'ch2_ils_l0a_' + masterCSV + os.sep + pre_dateFD_struct + dd[0]+os.sep+dd[1]+os.sep+dd[2]+ os.sep
    if not os.path.exists(fs):
        os.makedirs(fs)
        #xmlPaths.append(fs)
    else:
        print("Date Folder already exists: "+ fs)
    return fs

# Start of Generating XML files for created baby CSV files in PDS4 data structure

def get_MD5_CheckSum(file):
    return hashlib.md5(open(file, 'rb').read()).hexdigest()

def prettify(elem):
    """Returns a pretty-printed XML String for the Element."""
    xml_firstLine = '''<?xml version="1.0" encoding="UTF-8" standalone="no"?><?xml-model href="PDS4_SP_1001.sch" schematypens="http://purl.oclc.org/dsdl/schematron"?>'''
    rough_string = ElementTree.tostring(elem, "utf-8")
    reparsed = minidom.parseString(rough_string)
    finalXML = reparsed.toprettyxml(indent=" ").split('<?xml version="1.0" ?>')[1]
    return xml_firstLine + finalXML

def get_BabyCSV_info(babyCSVPD):  # Baby CSV Pandas structure
    temp_str = ""
    for c in babyCSVPD.columns:
        temp_str = temp_str + c + ','
    header_col_len = len(temp_str)
    start_time = babyCSVPD['UTC Time'].iloc[0]
    end_time = babyCSVPD['UTC Time'].iloc[-1]  # last record
    no_of_records = babyCSVPD.__len__()
    file_size = babyCSVPD.size
    no_fields = len(babyCSVPD.columns)
    return (header_col_len, start_time, end_time, no_of_records, file_size, no_fields)

def get_column_str(req_cols):
    #req_cols.append('OBT')  # this line can be removed later
    col_string = ""
    for i in range(len(req_cols)):
        ss = '''<Field_Delimited><name>{}</name><field_number>{}</field_number><data_type>{}</data_type><unit>{}</unit><description>{}</description></Field_Delimited>'''.format(
            req_col_names[i], str(i + 1), data_types[i], units[i], col_discription_dict[req_col_names[i]])
        col_string = col_string + ss
    #req_cols.remove('OBT')  # this line can be removed later
    return col_string

def writeXML(babyCSVFilePath):
    """This function creates PDS4 structure in xml for required columns"""
    prod_obs_attr = {"xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                     "xsi:schemaLocation": "http://pds.nasa.gov/pds4/pds/v1 PDS4_PDS_1B00.xsd",
                     "xmlns": "http://pds.nasa.gov/pds4/pds/v1"}
    base, fname = os.path.split(babyCSVFilePath)
    csv_name = babyCSVFilePath.rsplit('\\', 1)[1].split('.')[0]  # extracting file without extension
    babyPD = pd.read_csv(babyCSVFilePath)
    file_checksum_MD5 = get_MD5_CheckSum(babyCSVFilePath)
    header_col_len, start_time, end_time, no_of_records, file_size, no_fields = get_BabyCSV_info(babyPD)
    col_string = get_column_str(req_col_names)
    xml_String = '''<root><Identification_Area><logical_identifier>urn:isro:isda:ch2_chl:ils_raw:''' + csv_name + '''</logical_identifier><version_id>1.0</version_id><title>CH2 ILSA STUDIES</title><information_model_version>''' + info_model_version + '''</information_model_version><product_class>Product_Observational</product_class><Modification_History><Modification_Detail><modification_date>2018-08-03</modification_date><version_id>1.0</version_id><description>PDS4 product label created by ILSA DP team</description></Modification_Detail></Modification_History></Identification_Area><Observation_Area><Time_Coordinates><start_date_time>''' + start_time + '''</start_date_time><stop_date_time>''' + end_time + '''</stop_date_time></Time_Coordinates><Primary_Result_Summary><purpose>Science</purpose><processing_level>Raw</processing_level><description>Science Data</description></Primary_Result_Summary><Investigation_Area><name>Chandrayaan-2</name><type>Mission</type><Internal_Reference><lidvid_reference>urn:isro:isda:context:investigation:mission.chandrayaan2::1.0</lidvid_reference><reference_type>data_to_investigation</reference_type></Internal_Reference></Investigation_Area><Observing_System><name>Vikram_ILSA</name><description>ILSA payload of Chandrayaan-2 Vikram (Lander)</description><Observing_System_Component><name>Vikram</name><type>Spacecraft</type><description>Chandrayaan-2 Mission Vikram (Lander)</description></Observing_System_Component><Observing_System_Component><name>ILSA</name><type>Instrument</type><description>Instrument for Lunar Seismic  Activity (ILSA) on Chandrayaan-2 lander mission, which is seismometer based payload characterizing the seismic activity of the moon around the landing site, delineating the strucutre of the lunar crust and mantle. Further artificial events such as rover movement, operations of payloads like ChASTE  and LIBS. This instrument is a three axis based coarse and fine sensor working in the range of +/-0.5g</description></Observing_System_Component></Observing_System><Target_Identification><name>Moon</name><type>Satellite</type><description>Moon is a natural satellite body of the earth.</description></Target_Identification></Observation_Area><File_Area_Observational><File><file_name>''' + csv_name + '.csv' + '''</file_name><creation_date_time>''' + datetime.isoformat(
        datetime.today())[:-3] + '''</creation_date_time><file_size unit="byte">''' + str(
        file_size) + '''</file_size><records>''' + str(
        no_of_records) + '''</records><md5_checksum>''' + file_checksum_MD5 + '''</md5_checksum></File><Header><name>Column headings for TABLE</name><local_identifier>header</local_identifier><offset unit="byte">0</offset><object_length unit="byte">''' + str(
        header_col_len) + '''</object_length><parsing_standard_id>PDS DSV 1</parsing_standard_id></Header><Table_Delimited><name>Lookout table for Level-0 data product</name><local_identifier>Table</local_identifier><offset unit="byte">''' + str(
        header_col_len + 1) + '''</offset><parsing_standard_id>PDS DSV 1</parsing_standard_id><description>Table contains the seismic data for coarse sensor and fine sensor along X,Y and Z axes.</description><records>''' + str(
        no_of_records) + '''</records><record_delimiter>Carriage-Return Line-Feed</record_delimiter><field_delimiter>Comma</field_delimiter><Record_Delimited><fields>''' + str(
        no_fields) + '''</fields><groups>0</groups>''' + col_string + '''</Record_Delimited></Table_Delimited></File_Area_Observational></root>'''
    parent = Element('Product_Observational', attrib=prod_obs_attr)
    children = XML(xml_String)
    parent.extend(children)
    parent = prettify(parent)
    with open(base + os.sep + csv_name + '.xml', 'w') as xml:
        xml.writelines(parent)

# End of Generating XML files for created baby CSV files in PDS4 data structure

# Start of Validating XMl files with validate tool
def run_win_cmd(cmd):
    print("CMD:" + cmd)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    errcode = process.returncode
    process.communicate()
    if errcode is not None:
        raise Exception('cmd %s failed, see above for details', cmd)

def validate_XML(xmlDir,mFile): # It will run without errors in Krishna PC, in other PC it need to modify
    if len(glob.glob(xmlDir+ "*.xml")) == 0:
        print("There are no XML files to process under: " + xmlDir)
        print("Program terminated...!!!!")
        sys.exit(0)
    fdate = xmlDir.rsplit('Calibrated')[1].replace('\\','_') #It is simple if Calibrated path is in correct position
    log_file_path = log_Dir + "Validation_log_"+ mFile + fdate + ".txt "
    run_win_cmd("validate4 -r " + log_file_path + xmlDir + "*.xml")
    with open(log_file_path, 'r') as fp:
        logInfo = fp.read().split('Validation Details:')[1]
    if 'FAIL:' not in logInfo:
        print("Validation Successful")
    else:
        print("Validation Failed..Something Went Wrong....!!!!")
    print(logInfo)

# End of Validating XMl files with validate tool

def zipDir(zipFileName,dir): # This function will not compress the files
    """Use this function if we need to process the files before zip"""
    zf = zipfile.ZipFile(zipFileName,'w',zipfile.ZIP_DEFLATED) #zipfile.ZIP_DEFLATED with this it will compress
    for dirname, subdirs, files in os.walk(dir):
        zf.write(dirname)
        for filename in files:
            zf.write(os.path.join(dirname,filename))
    zf.close()

def sliceData(slice_name, data1, day_end_index):
    sliceno = 0
    i = 0
    dummy_dict = {}
    while i < day_end_index:
        sliceno = sliceno + 1
        dummy_dict[slice_name + '_' + str(sliceno)] = data1[(data1['UTC Time'] >= data1['UTC Time'].iloc[i]) & (
        data1['UTC Time'] <= (data1['UTC Time'].iloc[i] + pd.to_timedelta('15m')))] #np.timedelta64(15, 'm')
        i = i + len(dummy_dict[slice_name + '_' + str(sliceno)])
    return dummy_dict
def generateCSV_XML(masterCSVFile): #generateCSV_XML()
    xmlPaths = []
    minSliceDict = {}
    temp_dict = {}
    test=True
    if test == True: # it is for testing (DEBUG)
        data = pd.read_csv(masterCSVFile, parse_dates=[1])
        masterCSVFileName = masterCSVFile.rsplit('\\', 1)[1].split('.')[0]
        req_col_names.append('OBT')
        data.columns = req_col_names
        #data_types = get_PDS_Types(data.dtypes)

    else:
        data = pd.read_csv(masterCSVFile, parse_dates=[2])
        masterCSVFileName = masterCSVFile.rsplit('\\',1)[1].split('.')[0]
        data = data.iloc[:, req_col_positions]
        data.columns = req_col_names
        data['OBT'] = [d.timestamp() for d in data['UTC Time']]  #For testing generated OBT column
        req_col_names.append('OBT')
    #data['UTC Time'] = pd.date_range('2019-01-23T23:35:00.000Z', periods=len(data['UTC Time']), freq='5L') # for testing timestamp generated
    data_types.extend(get_PDS_Types(data.dtypes))
    days = pd.unique(data['UTC Time'].dt.date)
    for d in days:
        temp_dict['data_on_' + str(d)] = data[data['UTC Time'].dt.date == d]

    for key in temp_dict.keys():
        day_len = len(temp_dict[key])
        day_data = temp_dict[key]
        minSliceDict.update(sliceData(key, day_data, day_len))

    for key in minSliceDict.keys():
        print(len(minSliceDict[key]))
        minSliceDict[key].iloc[:, 1] = [datetime.strftime(d, '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' for d in minSliceDict[key].iloc[:, 1]]
        dateFolderPath = createDateFolder(masterCSVFileName, key.split('_')[2]) #Here I am passing master file also to create separate folder with Mst_CSV
        if dateFolderPath not in xmlPaths:
            xmlPaths.append(dateFolderPath)
        babyCSV_Name = dateFolderPath + key + '.csv'  # Todo need to add logic
        (minSliceDict[key]).to_csv(babyCSV_Name, index=False, date_format='iso')  # HERE WE CAN WRITE LOGIC TO ELIMINATE DUPLICATE CREATION OF BABY FILE
        writeXML(babyCSV_Name)
    print("Baby CSV and XML file generation successful..")
    print("Please wait validation is running on generated XMLs")
    for xmlPath in xmlPaths:
        validate_XML(xmlPath,masterCSVFileName) #should pass day folder path
    dir_to_zip = output_Dir + 'ch2_ils_l0a_' + masterCSVFileName  # it was already created in createDateFolder()
    zfName = dir_to_zip+".zip"
    print("Taring/Zipping the generated xml and csv files...........!!!!!!")
    shutil.make_archive(zfName, 'tar', dir_to_zip)
    req_col_names.remove('OBT')
    # zipDir(zfName, dir_to_zip) # I Don't know zipDir function not working

def ilsaDataArchive(inputDir): # Todo Need to write code to read tar, zip and gz files
    # Already ensured files will be present in input_Dir
    for filepath in glob.glob(inputDir + '*.csv'): # processing each Master CSV file
        if os.path.isfile(filepath):
            print("Processing: "+ os.path.split(filepath)[1])
            generateCSV_XML(filepath)

def read_tar_zip_bz(filepath): # Todo need to change input directory
    """It will returns all the csv files from filepath:.tar,.zip,.bz2"""
    csvFiles = [] # declaration not required but just keeping
    if filepath.endswith('.zip'):
        zf = zipfile.ZipFile(filepath, 'r')
        csvFiles = [file for file in zf.namelist() if file.endswith('.csv')]
        zf.close()
    elif filepath.endswith('.tar') or filepath.endswith('.tar.gz') or filepath.endswith('.tar.tgz'):
        tar = tarfile.open(filepath,'r:gz')
        csvFiles = [file for file in tar.getnames() if file.endswith('.csv')]
        tar.close()
    elif filepath.endswith('.bz2') or filepath.endswith('.tbz'):
        bz = tarfile.open(filepath,'r:bz2')
        csvFiles = [file for file in bz.getnames() if file.endswith('.csv')]
        bz.close()
    return csvFiles

def readInputFiles():
    """This function will read the *.tar, *.zip files"""
    pass

ilsaDataArchive(input_Dir)