using NLog;

namespace Prob_Tst_ETL
{
    class LogPerformance
    {
        string myMsg;
        public LogPerformance(string message)
        {
            myMsg = message;
                    
        }
        public void LogMe()
        {
            Logger logger = LogManager.GetCurrentClassLogger();
            logger.Info(myMsg);
        }
    }
}
