using System;
using NLog;

namespace Prob_Tst_ETL
{
    public class LogException
    {
        public LogException(string message, Exception ex)
        {
            Logger logger = LogManager.GetCurrentClassLogger();
#pragma warning disable CS0618 // Type or member is obsolete
            logger.ErrorException(message, ex.GetBaseException());
#pragma warning restore CS0618 // Type or member is obsolete

        }

    }
}
