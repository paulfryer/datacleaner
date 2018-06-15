
var AWS = require('aws-sdk');

var athena = new AWS.Athena();
var firehose = new AWS.Firehose();
var ssm = new AWS.SSM();


const sleep = require('util').promisify(setTimeout);

exports.handler = async (event, context, callback) => {
 
 
    if (!event.QueryOutputLocation) {
        var getLocation = await ssm.getParameter({Name: '/Cleaner/QueryOutputLocation'}).promise();
        event.QueryOutputLocation = getLocation.Parameter.Value; }
    if (!event.DeliveryStreamName) {
        var getStream = await ssm.getParameter({Name: '/Cleaner/DeliveryStreamName'}).promise();
        event.DeliveryStreamName = getStream.Parameter.Value; }
 
   if (!event.Iteration)
       event.Iteration = 0;
   
   event.Iteration++;
   
   if (event.Iteration == 1)
   {
       event.RecordsProcessed = 0;
        event.NextToken = "FIRST";

       
    var params = {
      QueryString: event.QueryString,
      ResultConfiguration: {
        OutputLocation: event.QueryOutputLocation, 
        EncryptionConfiguration: {
          EncryptionOption: 'SSE_S3'
        }
      },
      QueryExecutionContext: {
        Database: event.Database
      }
    };
    var queryResult = await athena.startQueryExecution(params).promise();
    event.QueryExecutionId = queryResult.QueryExecutionId;
   }
    
    await waitForQueryToFinish(event.QueryExecutionId);
    await writeResultsToFirehose(event, context);

    event.HasMoreRecords = event.NextToken != null;

    return event;
};

async function writeResultsToFirehose(event, context)
{
    var firehoseTasks = [];
    var isFirstRow = true;
            
    while (event.NextToken)
    {
        var queryParams = {
            QueryExecutionId: event.QueryExecutionId,
            MaxResults: 1000
        };
        if (event.NextToken != "FIRST")
            queryParams.NextToken = event.NextToken;
        var results = await athena.getQueryResults(queryParams).promise();
        event.NextToken = results.NextToken;
        var properties = [];
        var records = [];
        results.ResultSet.Rows.forEach(row => {
            if (isFirstRow && event.Iteration == 1)
                row.Data.forEach(col => properties.push(col.VarCharValue));
            else {
                var record = {};
                var i = 0;
                row.Data.forEach(col => {
                    var cleanValue = cleanData(col.VarCharValue);
                    record[properties[i]] = cleanValue;
                    i++;
                });
                records.push({Data: JSON.stringify(record) + "\n"});
            }
            isFirstRow = false;
        });
        
        if (records.length > 500)
        {
            var set1 = records.slice(0, 500);
            var set2 = records.slice(500);
            firehoseTasks.push(firehose.putRecordBatch({DeliveryStreamName: event.DeliveryStreamName, Records: set1}).promise());
            firehoseTasks.push(firehose.putRecordBatch({DeliveryStreamName: event.DeliveryStreamName, Records: set2}).promise());
        } else {
            firehoseTasks.push(firehose.putRecordBatch({DeliveryStreamName: event.DeliveryStreamName, Records: records}).promise());
        }
        console.log("processed " + records.length + " records.");
        event.RecordsProcessed += records.length;
        if (context.getRemainingTimeInMillis() < 5000)
        {
            console.log("Only 5 seconds or less left so exiting processing loop.");
            break;
        }
    }
    await Promise.all(firehoseTasks);
}

async function waitForQueryToFinish(queryExecutionId)
{
    var statusCode = "UNKNOWN";
    var queryResult;
    while (statusCode != "SUCCEEDED")
    {
        queryResult = athena.getQueryExecution({
            QueryExecutionId: queryExecutionId
        }).promise();
        var results = await Promise.all([queryResult, sleep(1000)]);
        statusCode = results[0].QueryExecution.Status.State;
        console.log("Execution status: ", statusCode);
    }
    return;
}

function cleanData(value){
    return value + "..cleaned";
}