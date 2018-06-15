var AWS = require('aws-sdk');
var glue = new AWS.Glue();
var ssm = new AWS.SSM();
exports.handler = async (event, context, callback) => {
    var getName = await ssm.getParameter({Name: '/Cleaner/CrawlerName'}).promise();
    var params = {
      Name: getName.Parameter.Value
    };
    await glue.startCrawler(params).promise();
    event.CrawlerStarted = true;
    return event;
};