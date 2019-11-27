using System;

namespace Prob_Tst_ETL
{
    class Send_Email_Body
    {
        public object[] arrayMsg = new object[200];

        public Send_Email_Body(string Body, string District)
        {

            int i = 0; //Counter to iterate through result.ErrorMessages after calling the Api

            //Calling MAC notification engine
            
            var result = MacApiClient.Service.GenerateNotifications("API1", body: Body, district: District);
            Console.WriteLine($"Sending email notification to District: {District} was {result.SuccessCode}");
            
            foreach (var msg in result.ErrorMessages)
            {
                LogException logException = new LogException($"Sending email notification to District: {District} was {result.SuccessCode}", new Exception(msg, null));
                arrayMsg[i] = msg;
                //Console.WriteLine($"Error Message: {arrayMsg[i]}");
                i++;
            }

            
            
        }
    }
}
