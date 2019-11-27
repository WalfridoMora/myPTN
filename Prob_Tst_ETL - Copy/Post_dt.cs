using System;
using System.Data;
using System.Data.SqlClient;

namespace Prob_Tst_ETL
{
    class Post_dt
    {
        public string myError ="";
        public Post_dt(DataTable dt, string DestinationTableName, SqlConnection myConnection)
        {
            using (SqlBulkCopy bulkCopy1 = new SqlBulkCopy(myConnection))
            {
                foreach (System.Data.DataColumn c in dt.Columns)
                     bulkCopy1.ColumnMappings.Add(c.ColumnName, c.ColumnName);
                     bulkCopy1.DestinationTableName = DestinationTableName;
                try
                {
                    bulkCopy1.WriteToServer(dt);
                }
                catch (Exception ex)
                {
                    myError = ex.Message.ToString();
                }
                bulkCopy1.Close();
            }
        }
    }
}
