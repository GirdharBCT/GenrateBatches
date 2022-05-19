using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GenrateBatches
{
    public class Function
    {
        private readonly IAmazonAthena AthenaClient;

        public Function()
        {
            AthenaClient = new AmazonAthenaClient();
        }

        public Function(IAmazonAthena amazonAthena)
        {
            this.AthenaClient = amazonAthena;
        }
        //public static object get_var_char_values(object d)
        //{
        //    return (from obj in d["Data"] select obj["VarCharValue"]).ToList();
        //}
        public class QueryExecutionResponse
        {
            public string Status { get; set; }
            public ResultSet Result { get; set; }
            public bool Finish { get; set; }
        }
        public class BatchItems
        {
            public int Offset { get; set; }
            public int Limit { get; set; }
            public int BatchNumber { get; set; }
        }

        public BatchItems[] Generate_batches(int rowcount)
        {
            BatchItems[] batches = new BatchItems[] { };
            var batchSize = 10;
            var batchNumber = 0;
            while (rowcount > 0)
            {
                batches.Append<BatchItems>(new BatchItems
                {
                    Offset = batchSize * batchNumber,
                    Limit = batchSize,
                    BatchNumber = batchNumber + 1
                }
                    );
                rowcount -= batchSize;
                batchNumber++;
            }
            return batches;
        }

        public async Task<QueryExecutionResponse> Get_query_execution_result(string query)
        {
            Console.WriteLine(query);
            var response_query_execution_id = await AthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest
            {
                QueryString = query,
                QueryExecutionContext = new QueryExecutionContext
                {
                    Database = "demo-dvt-db"
                },
                ResultConfiguration = new ResultConfiguration
                {
                    OutputLocation = "s3://demo-dvt-glue-bucket/result/"
                }
            }
                );
            //var response_get_query_details = await AthenaClient.GetQueryExecutionAsync(new GetQueryExecutionRequest
            //{
            //    QueryExecutionId = response_query_execution_id.QueryExecutionId
            //}
            //    );

            var status = "RUNNING";
            var iterations = 360;

            while (iterations > 0)
            {
                iterations--;
                var response_get_query_details = await AthenaClient.GetQueryExecutionAsync(new GetQueryExecutionRequest
                {
                    QueryExecutionId = response_query_execution_id.QueryExecutionId
                }
                    );

                status = response_get_query_details.QueryExecution.Status.State;

                if (status == "FAILED" || status == "CANCELLED")
                {
                    var failure_reason = response_get_query_details.QueryExecution.Status.StateChangeReason;
                    LambdaLogger.Log(failure_reason);
                    return new QueryExecutionResponse
                    {
                        Status = status
                    };
                }
                else if (status == "SUCCEEDED")
                {
                    var location = response_get_query_details.QueryExecution.ResultConfiguration.OutputLocation;
                    //# Function to get output results
                    var response_query_result = await AthenaClient.GetQueryResultsAsync(new GetQueryResultsRequest
                    {
                        QueryExecutionId = response_query_execution_id.QueryExecutionId
                    });
                    var result_data = response_query_result.ResultSet;
                    return new QueryExecutionResponse
                    {
                        Status = status,
                        Result = result_data
                    };
                }

                //Thread.Sleep(1);
                await Task.Delay(1);

            }
            return new QueryExecutionResponse
            {
                Finish = true
            };
        }

        public BatchItems[] FunctionHandler(ILambdaContext context)
        {
            var query = "SELECT COUNT(*) FROM \"demo-dvt-db\".\"50_contacts_csv\"";
            var response_query_result = Get_query_execution_result(query);
            LambdaLogger.Log($"{response_query_result}");
            var countrow = 0;

            if (response_query_result.IsCompletedSuccessfully)
            {
                if (response_query_result.Result.Result.Rows.Count > 1)
                {
                    countrow = response_query_result.Result.Result.Rows[1].Data[0].VarCharValue.Length;
                    LambdaLogger.Log($"{ countrow}");
                }
            }
            return Generate_batches(countrow);
        }
    }
}
