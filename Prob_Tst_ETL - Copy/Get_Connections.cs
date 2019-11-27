using FEL5.Core.Client.Logging;

namespace Prob_Tst_ETL
{
    class Get_Connections
    {
        public string MacDWCon; 
        public string SMODWCon; 

        

        //Here are the 2 dbconnect labels:
        //-	MACDWPTN_R note: This label have read access to MACDWSQL1 database
        //-	MACPTNDW_U note: this connection has read/write access to SMODWSQL1

        //- Both labels are linked to smo.ptn service



        public Get_Connections ()
        {
            
            //Use the logging delegate to hook all internal logging by the service calls into your logging mechanism
            LoggingManager.LogMessageDelegate = (MessageType type, string message, string loggerName) =>
            {
                //Console.WriteLine(message);
            };

            //Getting connection strings
            var connection1 = FEL5.DBConnect.Client.Service.GetConnectionString("MACDWPTN_R");
            MacDWCon = connection1?.FullConnectionString;
            
            var connection2 = FEL5.DBConnect.Client.Service.GetConnectionString("MACPTNDW_U");
            SMODWCon = connection2?.FullConnectionString;
                      

        }
    
        
    }
}
