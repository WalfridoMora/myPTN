using System;
using System.Data;
using System.Data.SqlClient;

namespace Prob_Tst_ETL
{
    class Get_dtRecord
    {
        public void createTmpTbl(SqlConnection sqlConnection, string strCmdTempTbl, string message)
        {
            using (SqlCommand sqlCmdTempTbl = new SqlCommand())
            {
                sqlCmdTempTbl.Connection = sqlConnection;                                  
                sqlCmdTempTbl.CommandType = CommandType.Text;
                sqlCmdTempTbl.CommandText = strCmdTempTbl;
                using (SqlDataAdapter sqlDataAdap = new SqlDataAdapter(sqlCmdTempTbl))
                {
                    sqlDataAdap.SelectCommand.CommandTimeout = 500;
                    try
                    {
                        sqlCmdTempTbl.ExecuteNonQuery();
                        
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        LogException logException = new LogException(message, ex);
                    }

                }              
                
                
            }  
              
        }

        public DataTable dtRec(SqlConnection sqlConnection, string strCmd, string message)
        {
            using (SqlCommand sqlCmd = new SqlCommand())
            {
                sqlCmd.Connection = sqlConnection;
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.CommandText = strCmd;
                using (SqlDataAdapter sqlDataAdap = new SqlDataAdapter(sqlCmd))
                {
                    sqlDataAdap.SelectCommand.CommandTimeout = 500;  // seconds
                    using (DataTable dtRecord = new DataTable())
                    {
                        try
                        {
                            sqlDataAdap.Fill(dtRecord);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            LogException logException = new LogException(message, ex);
                        }

                        return dtRecord;
                    }

                        
                }
                
            }
            

        }








    }
}