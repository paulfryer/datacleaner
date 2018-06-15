var AWS = require('aws-sdk');
var glue = new AWS.Glue();
var ssm = new AWS.SSM();
var stepfunctions = new AWS.StepFunctions();


exports.handler = async (event, context) => {
    
    if (!event.DatabaseName) {    
        var paramResult = await ssm.getParameter({Name: '/Cleaner/GlueDatabaseName'}).promise();
        event.DatabaseName = paramResult.Parameter.Value;  }
    
    if (!event.MaxResults || event.MaxResults <= 0)
        event.MaxResults = 2;
    
    var params = {
      DatabaseName: event.DatabaseName,
      MaxResults: event.MaxResults,
      NextToken: event.NextToken
    };
    
    var tableResult = await glue.getTables(params).promise();
    
    var tasks = [];
    tableResult.TableList.forEach(table => {
       tasks.push(indexTable({DatabaseName: event.DatabaseName, TableName: table.Name}));
    });
    
    await Promise.all(tasks);
    
    event.NextToken = tableResult.NextToken;
    
    event.HasMoreTables = (tableResult.NextToken != null);
    
    
    return event;
};

var cleanerStateMachineArn = null;

async function indexTable (event)
{
    
    var tasks = [
        glue.getTable({DatabaseName: event.DatabaseName, Name: event.TableName}).promise(),
        glue.getPartitions(event).promise(),
        ssm.getParameter({Name: '/Cleaner/CleanerStateMachineArn'}).promise() ];
    
    var results = await Promise.all(tasks);
    
    var tableResult = results[0];
    var partitionResult = results[1];
    var ssmResult = results[2];
        
    cleanerStateMachineArn = ssmResult.Parameter.Value;
    
    tasks = [];
    var partitionKeys = tableResult.Table.PartitionKeys;

    var sql = "select * from " + event.TableName;


    if (partitionKeys.length == 0)
    {
        // TODO: check the table for theProcessedByCleaner property.
        
          var params = {
              stateMachineArn: cleanerStateMachineArn,
              input: JSON.stringify({
                 QueryString: sql,
                 Database: event.DatabaseName
              }),
              name: event.DatabaseName + "_" + event.TableName
            };
            tasks.push(stepfunctions.startExecution(params).promise());
    }
    else
    // TODO: add paging support.
    partitionResult.Partitions.forEach(partition =>  {
       if (!partition.Parameters.ProcessedByCleaner)
       {
           var i = 0;
           partitionKeys.forEach(partitionKey => {
               if (i == 0)
                 sql = sql  + " where ";
               else
                 sql = sql + " and ";
                 
               sql = sql + partitionKey.Name + " = '" + partition.Values[i] + "'";
               i++;
           });
  
           var params = {
              stateMachineArn: cleanerStateMachineArn,
              input: JSON.stringify({
                 QueryString: sql,
                 Database: event.DatabaseName
              }),
              name: event.DatabaseName + "_" + event.TableName + "_" + partition.Values.join('-')
            };
            tasks.push(stepfunctions.startExecution(params).promise());
           
           
        delete partition.DatabaseName;
        delete partition.TableName;
        delete partition.CreationTime;
       
           partition.Parameters.ProcessedByCleaner = 'True';
           
           
           var params2 = {
              DatabaseName: event.DatabaseName, 
              PartitionInput: partition,
              PartitionValueList: partition.Values,
              TableName: event.TableName 
                };
            tasks.push(glue.updatePartition(params2).promise());
           
       }
       
    });
    return Promise.all(tasks);
}