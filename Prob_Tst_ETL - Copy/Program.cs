#define Testing_MAC_Prod_Source

//namespace Compliance_ETL (Prob_Tst_ETL)
using Prob_Tst_ETL;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using NLog;

namespace Compliance_ETL
{
    class Program
    {

        static string MACconnection;
        static string SMOconnection;              
        static void Main(string[] args)
        {
            DateTime Mydate = DateTime.Now; // Gets current date and time
            Stopwatch stopWatch = new Stopwatch();//For elapsed time --Whole script--
            Stopwatch stopWatchRead = new Stopwatch();//For elapsed time --Independent Queries Reading from MAC--
            Stopwatch stopWatchWrite = new Stopwatch();//For elapsed time --Independent Queries Writing to SMO Database--
            stopWatch.Start();

            //Get Connections
            Get_Connections myConnections = new Get_Connections();


#if Testing_MAC_Prod_Source
            MACconnection = "Data Source = DOT-WPSQL001-C.fdot.dot.state.fl.us\\BIPRODSQL; Initial Catalog = MACDWSQL1; Integrated Security = True";// MAC CONNECTION (REPORTING - PRODUCTION)
#else
            MACconnection = myConnections.MacDWCon + ";Connection Timeout=100"; //MACconnection string.
#endif
            SMOconnection = myConnections.SMODWCon + ";Connection Timeout=100"; //SMOconnection string.  
            


            Ptn_Utilities util = new Ptn_Utilities();

            GlobalDiagnosticsContext.Set("extractingFrom", $"{ util.GetMySubString(MACconnection, "Data Source")}");
            GlobalDiagnosticsContext.Set("loadingTo", $"{ util.GetMySubString(SMOconnection, "Data Source")}");
            
            using (SqlConnection MACDWSQL1_Connection = new SqlConnection(MACconnection))// MAC DW CONNECTION 
            {
                using (SqlConnection SMODWSQL1_Connection = new SqlConnection(SMOconnection)) // SMO DW CONNECTION
                {
                    MACDWSQL1_Connection.Open();
                    SMODWSQL1_Connection.Open();
                    Console.WriteLine("*************************** " + Mydate + " ************************************");
                    Console.WriteLine("Extracting data From:");
                    Console.WriteLine($"{ util.GetMySubString(MACconnection, "Data Source")}");//Source
                    Console.WriteLine($"{ util.GetMySubString(MACconnection, "Initial Catalog")}");//Source
                    Console.WriteLine();
                    Console.WriteLine("Loading data To:");
                    Console.WriteLine($"{ util.GetMySubString(SMOconnection, "Data Source")}");//Target
                    Console.WriteLine($"{ util.GetMySubString(SMOconnection, "Initial Catalog")}");//Target
                    Console.WriteLine();
                    Console.WriteLine(util.GetMySubString(SMOconnection, "User ID"));// Connection User Id
                    Console.WriteLine($"[WINDOWS USER NAME]: {Environment.UserName}");// Windows User Name
                    Console.WriteLine("***********************************************************************************");
                    Console.WriteLine("***********************PROBLEM TEST REPORT IS RUNNING ETL JOBS*********************");
                    Console.WriteLine("************************************PLEASE WAIT************************************");
                    Console.WriteLine("***********************************************************************************");
                                       

                    Get_queryStr GetQuery = new Get_queryStr();

                    Get_dtRecord gr = new Get_dtRecord();

                    //*****************EMPTY HELPER TABLES**************************BEGIN * ************************

                    gr.createTmpTbl(SMODWSQL1_Connection, GetQuery.myQuery(1), "Query 1");

                    gr.createTmpTbl(SMODWSQL1_Connection, GetQuery.myQuery(2), "Query 2");

                    gr.createTmpTbl(SMODWSQL1_Connection, GetQuery.myQuery(3), "Query 3");

                    gr.createTmpTbl(SMODWSQL1_Connection, GetQuery.myQuery(4), "Query 4");

                    gr.createTmpTbl(SMODWSQL1_Connection, GetQuery.myQuery(5), "Query 5");
                    
                    Console.WriteLine("                         Helper tables successfully emptied");

                    //*****************EMPTY HELPER TABLES**************************END*************************  

                    Console.WriteLine("                         Extracting data from MAC...");
                    Console.WriteLine();
                    Console.WriteLine("*************************Updating helper tables************************************");
                    Console.WriteLine();

                    //***************************UPDATE QCP_ProdInfo***************BEGIN
                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    
                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(7), "Query 7"))
                    {                        
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        Post_dt post_dt_QCP_ProdInfo = new Post_dt(dt, "AGGREG.PTNTB0012_QCP_PROD_INFO", SMODWSQL1_Connection);
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_7 = new LogPerformance($"Query No. 7  | QCP Prod Info| Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_QCP_ProdInfo.myError == "")
                        {
                            Console.WriteLine("QCP Prod Info|-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_7.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_QCP_ProdInfo.myError);
                        }
                        
                    }
                    
                    //***************************UPDATE QCP_ProdInfo***************END
                                        
                    //*****************GET PRODUCTS ****************************BEGIN*********************************************************
                    
                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(8), "Query 8"))
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        dt.Columns.Add("DT", typeof(DateTime));
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["DT"] = Mydate;
                        }
                        Post_dt post_dt_PROD = new Post_dt(dt, "AGGREG.PTNTB0014_PRODUCTS", SMODWSQL1_Connection);
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_8 = new LogPerformance($"Query No. 8  | Agg. Products| Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_PROD.myError == "")
                        {
                            Console.WriteLine("Agg. Products|-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_8.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_PROD.myError);
                        }
                        

                    }                                        
                    //*****************GET PRODUCTS**************************END*******************************

                    //*****************GET MINES TO TERMINALS**************************BEGIN*************************
                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(9), "Query 9"))
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        var index = 1;
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["ID"] = index;
                            index++;
                        }
                        Post_dt post_dt_TERM = new Post_dt(dt, "AGGREG.PTNTB0015_MINE_TO_TERMINALS", SMODWSQL1_Connection);
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_9 = new LogPerformance($"Query No. 9  | Mine-Terminal| Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_TERM.myError == "")
                        {
                            Console.WriteLine("Mine-Terminal|-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_9.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_TERM.myError);
                        }
                        
                    }
                    //*****************GET MINES TO TERMINALS**************************END*************************
                    Console.WriteLine();
                    Console.WriteLine("***********************Running 30**************************************************");
                    Console.WriteLine();

                    //**********************GET ALL AGGREGATE SAMPLES, STATUS = COMPLETED, 1 YEAR OLD MAXIMUN, AT SOURCE****************END******                                                                                  
                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    Get_dtRecord gr_TMP_Tbls = new Get_dtRecord();
                    gr_TMP_Tbls.createTmpTbl(MACDWSQL1_Connection, GetQuery.myQuery(10), "Query 10"); // -->> -->>##PTNTB0070_30_SMPLS
                    stopWatchRead.Stop();
                    Console.WriteLine("Sample info  |-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                    LogPerformance logPerformance_10 = new LogPerformance($"Query No. 10 | Sample info  | Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                    logPerformance_10.LogMe();

                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    gr.createTmpTbl(MACDWSQL1_Connection,//Query 12 -->>##PTNTB0071_30_SMPLS_FM1T011
                        GetQuery.myQuery(12), "Query 12");
                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(11), "Query 11")) // Get FM1T011 compliance
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        dt.Columns.Add("DT", typeof(DateTime));
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["DT"] = Mydate;
                        }
                        Post_dt post_dt_FM1T011 = new Post_dt(dt, "AGGREG.PTNTB0010_COMPLIANCE", SMODWSQL1_Connection); //Load FM1T011 compliance
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_11 = new LogPerformance($"Query No. 11 | FM1T011      | Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_FM1T011.myError == "")
                        {
                            Console.WriteLine("FM1T011      |-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_11.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_FM1T011.myError);
                        }
                        

                    }                    

                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    gr.createTmpTbl(MACDWSQL1_Connection, //Query 14 -->> ##PTNTB0072_30_SMPLS_T27AggGrad
                        GetQuery.myQuery(14), "Query 14");
                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(13), "Query 13")) //Get T27AggGrad compliance
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        dt.Columns.Add("DT", typeof(DateTime));
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["DT"] = Mydate;
                        }
                        Post_dt post_dt_T27AggGrad = new Post_dt(dt, "AGGREG.PTNTB0010_COMPLIANCE", SMODWSQL1_Connection); // Load T27AggGrad compliance
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_13 = new LogPerformance($"Query No. 13 | T27AggGrad   | Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_T27AggGrad.myError == "")
                        {
                            Console.WriteLine("T27AggGrad   |-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");

                            logPerformance_13.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_T27AggGrad.myError);
                        }
                        
                    }
                    
                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    gr.createTmpTbl(MACDWSQL1_Connection, GetQuery.myQuery(10), "Query 10"); // Reload ##PTNTB0070_30_SMPLS
                    gr.createTmpTbl(MACDWSQL1_Connection, //Query 16 -->>##PTNTB0073_30_SMPLS_FM1T084FASG
                        GetQuery.myQuery(16)
                      , "Query 16");

                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(15), "Query 15")) // Get FM1T084FASG compliance
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        dt.Columns.Add("DT", typeof(DateTime));
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["DT"] = Mydate;
                        }
                        Post_dt post_dt_FM1T084FASG = new Post_dt(dt, "AGGREG.PTNTB0010_COMPLIANCE", SMODWSQL1_Connection);// Load FM1T084FASG compliance
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_15 = new LogPerformance($"Query No. 15 | FM1T084FASG  | Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_FM1T084FASG.myError == "")
                        {
                            Console.WriteLine("FM1T084FASG  |-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_15.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_FM1T084FASG.myError);
                        }
                        

                    }
                    
                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    gr.createTmpTbl(MACDWSQL1_Connection, //Query 19 -->>##PTNTB0074_30_SMPLS_FM1T085CASG
                        GetQuery.myQuery(19), "Query 19");

                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(18), "Query 18")) //Get FM1T085CASG compliance
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        dt.Columns.Add("DT", typeof(DateTime));
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["DT"] = Mydate;//Update the real Date for Running Thirty
                        }
                        Post_dt post_dt_FM1T085CASG = new Post_dt(dt, "AGGREG.PTNTB0010_COMPLIANCE", SMODWSQL1_Connection);// Load FM1T085CASG compliance
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_18 = new LogPerformance($"Query No. 18 | FM1T085CASG  | Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_FM1T085CASG.myError == "")
                        {
                            Console.WriteLine("FM1T085CASG  |-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_18.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_FM1T085CASG.myError);
                        }
                        
                    }

                    stopWatchRead.Reset();
                    stopWatchRead.Start();
                    gr.createTmpTbl(MACDWSQL1_Connection,//Query 22 -->>##PTNTB0076_FM1T096_LIMITS
                        GetQuery.myQuery(22), "Query 22");
                    gr.createTmpTbl(MACDWSQL1_Connection, GetQuery.myQuery(20), "Query 20"); // -->> ##PTNTB0075_30_SMPLS_FM1T096
                    using (var dt = gr.dtRec(MACDWSQL1_Connection, GetQuery.myQuery(21), "Query 21")) // Get FM1T096 compliance
                    {
                        stopWatchRead.Stop();
                        stopWatchWrite.Reset();
                        stopWatchWrite.Start();
                        dt.Columns.Add("DT", typeof(DateTime));
                        foreach (System.Data.DataRow row in dt.Rows)
                        {
                            row["DT"] = Mydate;//Update the real Date for Running Thirty
                        }
                        Post_dt post_dt_FM1T096 = new Post_dt(dt, "AGGREG.PTNTB0010_COMPLIANCE", SMODWSQL1_Connection); //load FM1T096 compliance
                        stopWatchWrite.Stop();
                        LogPerformance logPerformance_21 = new LogPerformance($"Query No. 21 | FM1T096      | Read Time: {stopWatchRead.Elapsed.ToString()} | Write Time: {stopWatchWrite.Elapsed.ToString()}");
                        if (post_dt_FM1T096.myError == "")
                        {
                            Console.WriteLine("FM1T096      |-- Read Time: " + stopWatchRead.Elapsed.ToString() + " --|-- Write Time: " + stopWatchWrite.Elapsed.ToString() + " --|");
                            logPerformance_21.LogMe();
                        }
                        else
                        {
                            Console.WriteLine(post_dt_FM1T096.myError);
                        }
                        
                    }
                                      
                    gr.createTmpTbl(SMODWSQL1_Connection, GetQuery.myQuery(23), "Query 23"); //INSERTING A NEW ROW IN TABLE "AGGREG.PTNTB0016_COMPLIANCE_UPDATE" 
                    
                    //FIRES A TRIGGER (AFTER INSERT):
                    //TRIGGER[AGGREG].[PTNTG0001001_Insert_COMPLIANCE_UPDATE]
                    //ON[SMODWSQL1].[AGGREG].[PTNTB0016_COMPLIANCE_UPDATE]
                    //GET ALL PRODUCTS OUT OF COMPLIANCE BY DISTRICT
                    //GET AN HTML TABLE FROM EACH QUERY RESULT (BY DISTRICT)USING STORED PROCEDURE "AGGREG.PTNPR0001_QueryToHtmlTable"
                    //MODIFY THE HTML STRING ADDING HEADER, FOOTER, ETC. (SEE TRIGGER CODE)
                    //ONCE THE HTML EMAIL BODY IS BUILT WE STORE IT IN TABLE "AGGREG.PTNTB0018_EMAIL_BODIES"

                    stopWatch.Stop();
                    Console.WriteLine();
                    Console.WriteLine("                         ETL RunTime: " + stopWatch.Elapsed.ToString());
                    Console.WriteLine();
                    Console.WriteLine("*************************ETL Jobs have finished************************************");
                    Console.WriteLine("***********************************************************************************");
                    Console.WriteLine("*************************Waiting for SQL Server Updates****************************");
                    Console.WriteLine("***********************************************************************************");
                    Thread.Sleep(5000);
                    Console.WriteLine("*************************Calling MAC notification engine***************************");
                    Console.WriteLine("***********************************************************************************");                                                        
                    
                    using (var dt = gr.dtRec(SMODWSQL1_Connection, GetQuery.myQuery(24), "Query 24"))//GET THE EMAIL BODIES FROM [AGGREG].[PTNTB0018_EMAIL_BODIES] TO BE SENT VIA API
                    {
                        if (dt.Rows.Count > 0)
                        {
                            foreach (DataRow row in dt.Rows)
                            {
                                //SEND EMAIL BODY AND EVENT ID (DISTRICT) 
                                Send_Email_Body S_E_B = new Send_Email_Body(row["body"].ToString(), row["district"].ToString());
                                
                            }                            
                        }
                        else Console.WriteLine("No emails to be sent!!!");
                    }
                        
                    
                    
                    
                    MACDWSQL1_Connection.Close();
                    SMODWSQL1_Connection.Close();
                }// end -- using (SqlConnection MACDWSQL1_Connection = new SqlConnection(MACconnection))
            } // end -- using (SqlConnection SMODWSQL1_Connection = new SqlConnection(SMOconnection))
                       
            Thread.Sleep(3000); //Application closes in 3 seconds.
            Console.ReadKey();

        } //static void Main(string[] args)                   
    }//class Program 
}// namespace Compliance_ETL

