# NIM-System-PowerShell-DB2-SQL

# Requirements
- IBM DB2 ODBC DRIVER installed locally
    - Download Driver https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information#%5B%3Ch2%3E%5DDownload%5B%3C%2Fh2%3E%5D
	    - Specifically vXX.XX.XX_ntx64_odbc_cli
	- Create C:\Progam Files\IBM
	- Extract clidriver contents to C:\Program Files\IBM
	    - e.g "bin" folder should C:\Program Files\IBM
	- Open Command Prompt, run installation command
	`
		cd "C:\Program Files\IBM\bin"
		db2cli install -setup
	`
	

