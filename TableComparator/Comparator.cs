using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace TableComparator {

    /* The Comparator class is used to compare records from 2 different tables 
     * with an identical schema */
    class Comparator {

        private string sourceTable, destinationTable, primaryKey;

        private string connectionString = "Server=localhost\\SQLEXPRESS;Database=Randombase;Trusted_Connection=True";
        private string outputPath = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        private string[] outputfiles = { "uniqueSourceRecords.txt", "uniqueDestinationRecords.txt", "modifiedRecords.txt" };
        private SqlConnection connection;
        private bool storedChanges = false;


        public Comparator() {
            connection = new SqlConnection(connectionString);
            SetTableNames();
            ParseRecords(sourceTable, destinationTable, 0);
            storedChanges = true;
            ParseRecords(destinationTable, sourceTable, 1);
        }


        
        private void SetTableNames() {
            sourceTable = "TestTable1";
            destinationTable = "TestTable2"; 
            primaryKey = "UserID"; 
        }


        
        /* Find all the field values if there exists a record in table 'tableName' 
         * with a primary key value matching 'primaryKeyValue' */
        private List<string> FindSecondaryRecord(string tableName, string primaryKeyValue) {
            List<string> values = new List<string>();
            string queryString = "select * from " + tableName + " where " + primaryKey + " = @value";
            SqlConnection innerConnection = new SqlConnection(connectionString);
            try {
                innerConnection.Open();
                SqlCommand command = new SqlCommand(queryString, innerConnection);
                command.Parameters.AddWithValue("@primarykey", primaryKey);
                command.Parameters.AddWithValue("@value", primaryKeyValue);
                SqlDataReader reader = command.ExecuteReader();
                while(reader.Read()) {
                    for(int field = 0; field < reader.FieldCount; field++) {
                        string fieldName = reader.GetName(field).ToString();
                        string fieldValue = reader[fieldName].ToString();
                        values.Add(fieldValue);
                    }
                }
                reader.Close();
            }
            catch(Exception e) {
                Console.WriteLine("Exception: "+e.Message);
            }
            finally {
                innerConnection.Close();
            }
            return values;
        }



        /* Compare the content of two lists storing field values */ 
        private bool MatchingFields(List<string> primaryRecord, List<string> secondaryRecord) {
            if(primaryRecord.Count != secondaryRecord.Count) {
                return false;
            }
            int fields = primaryRecord.Count;
            for(int i=0; i<fields; i++) {
                if(!primaryRecord[i].Equals(secondaryRecord[i])){
                    return false;
                }
            }
            return true;
        }



        /* Parse each record of the primary table, and use the primary key value to examine whether a record exists in 
         * the secondary table as well */
        private void ParseRecords(string primaryTable, string secondaryTable, int outputFileIndex) {
            try {
                connection.Open();
                string queryString = "select * from " + primaryTable;
                SqlCommand command = new SqlCommand(queryString, connection);
                SqlDataReader reader = command.ExecuteReader();
                while(reader.Read()) {
                    string keyValue = reader[primaryKey].ToString();
                    List<string> secondaryRecord = FindSecondaryRecord(secondaryTable, keyValue);
                    List<string> primaryRecord = new List<string>();
                    for(int field = 0; field < reader.FieldCount; field++) {
                        string fieldName = reader.GetName(field).ToString();
                        string fieldValue = reader[fieldName].ToString();
                        primaryRecord.Add(fieldValue);
                    }
                    //If a matching record exists in the secondary table
                    if(secondaryRecord.Count > 0) {
                        //If any field of the record is modified in the destination table, write the modified record to file
                        if(!MatchingFields(primaryRecord, secondaryRecord) && !storedChanges) {
                            WriteToFile(outputfiles[2], secondaryRecord);
                        }
                    }
                    //Else write the primary record to file as a unique record
                    else {
                        WriteToFile(outputfiles[outputFileIndex], primaryRecord);
                    }
                }
                reader.Close();
            }
            catch(Exception e) {
                Console.WriteLine("Exception: " + e.Message);
            }
            finally {
                connection.Close();
            }
        }

    

        /* Write the field values stored in the recordData list to 
         * the specified file */
        private void WriteToFile(string fileName, List<string> recordData) {
            try {
                StreamWriter writer = new StreamWriter(outputPath + "\\" + fileName, true);
                foreach(string value in recordData) {
                    writer.Write(value + "\t");
                }
                writer.WriteLine();
                writer.Close();
            }
            catch(Exception e){
                Console.WriteLine("Exception: "+e.Message);
            }
        }
    }
}
