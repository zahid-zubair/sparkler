var MongoClient = require('mongodb').MongoClient;
var mongoDbConnectionUrl = "mongodb://localhost:27017/marketdb";
var http = require('http');
var url = require('url');
var dbResult;

http.createServer(function (req, res) {
    var q = url.parse(req.url.toString(), true);
    var qdata = q.query;
    
    MongoClient.connect(mongoDbConnectionUrl, function(err, db) {
      if (err) throw err;
      var stockName = qdata.name + '';
      var sortOrder = qdata.sortOrder;
      var collectionQuery = { "_id" : stockName };
      db.collection("marketStats").aggregate(
       [
        {
          $match: {
            "_id": stockName
          }
        },
        {
          $unwind: "$dates"
        },
        {
          $sort: {
            "dates.date": -1 
          }
        }
       ],
       function (err, result) { 
         console.log(result); 
       }
      );
      db.close();
    });
    res.write('test' + qdata.name);
    res.end();
}).listen(7070);

