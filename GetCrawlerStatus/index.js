var AWS = require('aws-sdk');
var glue = new AWS.Glue();
var ssm = new AWS.SSM();
exports.handler = async (event, context, callback) => {
    var getName = await ssm.getParameter({Name: '/Cleaner/CrawlerName'}).promise();
    var params = {
      Name: getName.Parameter.Value
    };
    var getState = await glue.getCrawler(params).promise();
    event.CrawlerState = getState.Crawler.State;
    if (!event.CrawlerStarted)
      event.CrawlerStarted = false;
    return event;
};