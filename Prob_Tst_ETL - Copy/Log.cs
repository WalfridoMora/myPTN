using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Prob_Tst_ETL
{
    class Log
    {

        public Log(byte id, SqlConnection myConnection, byte logType, string logTxt)// logTypes[] = {1:QueryExecError, 2:QueryPerformance, Other  } ---logTxt = "
        {
            switch (logType)
            {
                case 1:
                    Console.WriteLine("One");
                    break;
                case 2:
                    Console.WriteLine("Two");
                    
                    break;
                default:
                    Console.WriteLine("Other");
                    break;
            }
        }

    }
}
