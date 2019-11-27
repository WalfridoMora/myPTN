using System;

namespace Prob_Tst_ETL
{
    class Ptn_Utilities
    {
        public string GetMySubString(string myString, string startingWith)
        {
            
            int i = 1;
            string MySubString = "";
            string myError = "";
            try
            {
                
                int position = myString.IndexOf(startingWith);
                string sub = myString.Substring(position);
                char c = sub[0];
                MySubString = MySubString + c;
                while (c != ';')
                {
                    c = sub[i];
                    if (c != ';') MySubString = MySubString + c;
                    i++;
                }

            }
            catch(Exception ex)
            {
                myError = ex.Message.ToString();
            }
            
            if (myError =="")
            {
                return MySubString;
            }
            else
            {
                return myError;
            }
            

        }
    }
}
