<?php

ini_set('mysql.connect_timeout', 300);
ini_set('default_socket_timeout', 300);
ini_set('max_execution_time', 300);
putenv('TMPDIR=/tmp/');

$FTP_SERVER = "retsftp.realtracs.com";
$USERNAME 	= "Davenport";
$PASSWORD 	= "951885";

$DB_SERVER 		= "173.194.104.190";
$DB_USERNAME 	= "root";
$DB_PASSWORD 	= "MRSapp(!@#$)";
$DB_NAME 		= "myrealt8_mrs";

$EMAIL_TO 				= "aftabaig@gmail.com";
$EMAIL_FROM 			= "cron@mgdstuff.com";
$EMAIL_HEADERS 			= "From:" . $EMAIL_FROM;
$EMAIL_SUBJECT_SUCCESS 	= "CRON JOB RAN SUCCESSFULLY";
$EMAIL_SUBJECT_ERROR 	= "CRON JOB FAILED";

$RETS_FOLDER = @"/RETS_DECODED/";

$RES_DATA_FILE_PREFIX 	= "RES_IDX_DATA_DECODED_";
$CND_DATA_FILE_PREFIX 	= "CND_IDX_DATA_DECODED_";
$COM_DATA_FILE_PREFIX   = "COM_IDX_DATA_DECODED_";
$LLF_DATA_FILE_PREFIX   = "LLF_IDX_DATA_DECODED_";
$RNT_DATA_FILE_PREFIX   = "RNT_IDX_DATA_DECODED_";

//numerical columns for data tables.
$NUMERICAL_COLUMNS_DATA = array(
	"PictureCount",
	"SqFtTotal",
	"TotalFullBaths",
	"ListPrice",
	"TotalHalfBaths",
	"YearBuilt",
	"TotalBedrooms",
	"SqFtBasement",
	"SqFtSecondFloor",
	"Longitude",
	"Latitude",
	"TotalRooms",
	"SqFtThirdFloor",
	"SqFtMainFloor",
	"NumOfStories",
	"NumberOfFireplaces",
	"SqFtOther",
	"SecurityDeposit",
	"PetDeposit",
	"MinimumLease",
	"LeasePerMonth"
);

//primary columns for data tables.
$PRIMARY_COLUMNS_DATA = array(
	"MlsNum"
);

//connect to ftp.
//echo "Connecting to FTP ...\n";
$connection_id = ftp_connect($FTP_SERVER);
$response = ftp_login($connection_id, $USERNAME, $PASSWORD);
ftp_pasv($connection_id, true);
if ($response)
{
	//get contents of rets folder.
	$files  = ftp_nlist($connection_id, $RETS_FOLDER);

	$arr_res_data_file 	= preg_grep("/".$RES_DATA_FILE_PREFIX."(\w+)/", $files);
	$arr_cnd_data_file 	= preg_grep("/".$CND_DATA_FILE_PREFIX."(\w+)/", $files);
	$arr_com_data_file      = preg_grep("/".$COM_DATA_FILE_PREFIX."(\w+)/", $files);
	$arr_llf_data_file      = preg_grep("/".$LLF_DATA_FILE_PREFIX."(\w+)/", $files);
	$arr_rnt_data_file	= preg_grep("/".$RNT_DATA_FILE_PREFIX."(\w+)/", $files);

	//get required file names.
	$res_data_file 	= $arr_res_data_file[key($arr_res_data_file)];
	$cnd_data_file 	= $arr_cnd_data_file[key($arr_cnd_data_file)];
	$com_data_file 	= $arr_com_data_file[key($arr_com_data_file)];
	$llf_data_file 	= $arr_llf_data_file[key($arr_llf_data_file)];
	$rnt_data_file  = $arr_rnt_data_file[key($arr_rnt_data_file)];

	$arr_file_names = array();
	$arr_file_names[] = $res_data_file;
	$arr_file_names[] = $cnd_data_file;
	$arr_file_names[] = $com_data_file;
	$arr_file_names[] = $llf_data_file;
	$arr_file_names[] = $rnt_data_file;

	//clean folder before downloading
	//delete all files that may be present
	//from the previous download.
	$handle = opendir(".");
	while (FALSE !== ($entry = readdir($handle)))
	{
		if (endswith($entry, ".zip"))
	 	{
	 		unlink($entry);
		}
	}
	deleteDirectory("files");
	
	//download files.
	//echo "Downloading ...\n";
	foreach ($arr_file_names as $file_name)
	{
		//echo "Downloading $file_name\n";
		ftp_get($connection_id, $file_name, $RETS_FOLDER.$file_name, FTP_BINARY);	
	}

	//close ftp connection.
	ftp_close($connection_id);
	
	//unzip the files.
	//echo "Unzipping ....\n";
	foreach ($arr_file_names as $file_name)
	{
		//echo "Unzipping $file_name\n";
		$zip = new ZipArchive;
		$zip->open($file_name);
		$zip->extractTo("files");
		$zip->close();	
	}
	
	//connect to db.
	$db_connection = mysql_connect($DB_SERVER, $DB_USERNAME, $DB_PASSWORD);
	mysql_select_db($DB_NAME, $db_connection);

	if (mysqli_connect_errno())
	{
		mail($EMAIL_TO, $EMAIL_SUBJECT_ERROR, "Error connecting to database.", $EMAIL_HEADERS);
		exit(0);
	}

	//parse the xmls and inserting/updating the corresponding table.
	foreach ($arr_file_names as $file_name)
	{
		//echo "Update $file_name\n";

		$xml_file_name = str_replace("zip", "xml", $file_name);
		$res_data_xml = new DomDocument();
		$res_data_xml->load("files/".$xml_file_name);
		$x = $res_data_xml->documentElement;
		
		if (strpos($file_name, 'RES') !== FALSE)
		{
			$table_name = "tbl_data_res";
		}
		else if (strpos($file_name, 'CND') !== FALSE)
		{
			$table_name = "tbl_data_cnd";
		}
		else if (strpos($file_name, 'COM') !== FALSE)
		{
			$table_name = "tbl_data_com";
		}
		else if (strpos($file_name, 'LLF') !== FALSE)
		{
			$table_name = "tbl_data_llf";
		}
		else 
		{
			$table_name = "tbl_data_rnt";
		}
				
		//remove all entries from the table.
		$inactive_sql = "TRUNCATE TABLE `" . $table_name . "`";
		echo $inactive_sql;
		mysql_query($inactive_sql) or die("errror");

		$arr_columns = "";
		$i = 0;
		foreach ($x->childNodes as $item)
		{
			//found column names node.
			if ($item->nodeName === "COLUMNS")
			{
				//store column names into array.
				$str_columns = $item->nodeValue;
				$arr_columns = explode("\t", $str_columns);			
			}
			//found data node.
			else if ($item->nodeName === "DATA")
			{
				$i++;

				//store values into array.
				$str_values = $item->nodeValue;
				$arr_values = explode("\t", $str_values);

				$table_name 		= "";
				$numerical_columns 	= array();
				$primary_columns 	= array();
				$primary_values 	= array();

				//based on the file_name, set appropriate values for
				//table_name, numerical_columns, primary_columns and primary_values.
				$numerical_columns 	= $NUMERICAL_COLUMNS_DATA;
				$primary_columns 	= $PRIMARY_COLUMNS_DATA;

				if (strpos($file_name, 'RES') !== FALSE)
				{
					$table_name = "tbl_data_res";
				}
				else if (strpos($file_name, 'CND') !== FALSE)
				{
					$table_name = "tbl_data_cnd";
				}
				else if (strpos($file_name, 'COM') !== FALSE)
				{
					$table_name = "tbl_data_com";
				}
				else if (strpos($file_name, 'LLF') !== FALSE)
				{
					$table_name = "tbl_data_llf";
				}
				else 
				{
					$table_name = "tbl_data_rnt";
				}
				
				//get value for each primary column.
				foreach ($primary_columns as $column)
				{
					$index = array_search($column, $arr_columns);
					$primary_values[] = $arr_values[$index];
				}
				
				$insert_sql = insertSQL($table_name, $arr_columns, $arr_values, $numerical_columns);
				$update_sql = updateSQL($table_name, $arr_columns, $arr_values, $primary_columns, $primary_values, $numerical_columns);
				$sql = $insert_sql . " ON DUPLICATE KEY UPDATE " . $update_sql;
				$result = mysql_query($sql) or die(mysql_error());
			}
		}
	}

	mail($EMAIL_TO, $EMAIL_SUBJECT_SUCCESS, "Cron job was completed successfully.", $EMAIL_HEADERS);

}
else 
{
	mail($EMAIL_TO, $EMAIL_SUBJECT_ERROR, "Error connecting to ftp.", $EMAIL_HEADERS);
	exit(0);
}

function existsSQL($table_name, $columns, $values)
{
	$where = "";
	for ($i=0; $i<count($columns); $i++)
	{
		$where = $where . $columns[$i] . " = " . $values[$i];
		$where = $where . " AND ";
	}
	$where = $where . " TRUE = TRUE ";
	return "SELECT * FROM " . $table_name . " WHERE " . $where;
}

function insertSQL($table_name, $columns, $values, $numerical_columns)
{

	$str_columns = "";
	$str_values = "";
	$count = count($columns)-1;
	$last_column = $count-1;

	// we're skipping the first and the last row since
	// they just return '\t'
	for ($i=1; $i<$count; $i++)
	{
		$current_value = str_replace("'","\'",$values[$i]);

		$is_numerical = in_array($columns[$i], $numerical_columns);

		if ($is_numerical && $current_value === '')
		{
			$current_value = '0';
		}

		if ($columns[$i] === "LotSize" ||
			$columns[$i] === "MediaDescription" ||
			$columns[$i] === "URL")
		{
			$current_value = '';
		}

		if ($i == $last_column)
		{
			$str_columns = $str_columns . $columns[$i];
			$str_values = $str_values . "'" . $current_value . "'";
		}
		else 
		{
			$str_columns = $str_columns . $columns[$i] . ",";
			$str_values = $str_values . "'" . $current_value . "',";	
		}
	}
	return "INSERT INTO " . $table_name . " (".$str_columns.") VALUES (".$str_values.") ";
}

function updateSQL($table_name, $columns, $values, $primary_columns, $primary_values, $numerical_columns)
{
	$count = count($columns)-1;
	$last_column = $count-1;

	$set_sql = "";

	// we're skipping the first and the last row since
	// they just return '\t'
	for ($i=1; $i<$count; $i++)
	{
		$current_value = str_replace("'","\'",$values[$i]);
		$is_numerical = in_array($columns[$i], $numerical_columns);

		if ($is_numerical && $current_value === '')
		{
			$current_value = '0';
		}

		if ($columns[$i] === "LotSize" ||
			$columns[$i] === "MediaDescription" ||
			$columns[$i] === "URL")
		{
			$current_value = '';
		}

		$set_sql = $set_sql . $columns[$i] . " = '" . $current_value . "'";
		if ($i !== $last_column)
		{
			$set_sql = $set_sql . ", ";
		}
		
	}

	return $set_sql;
}

function endsWith($haystack, $needle)
{
    $length = strlen($needle);
    if ($length == 0) {
        return true;
    }

    return (substr($haystack, -$length) === $needle);
}

function deleteDirectory($dir) {
    if (!file_exists($dir)) return true;
    if (!is_dir($dir)) return unlink($dir);
    foreach (scandir($dir) as $item) {
        if ($item == '.' || $item == '..') continue;
        if (!deleteDirectory($dir.DIRECTORY_SEPARATOR.$item)) return false;
    }
    return rmdir($dir);
}

?>
