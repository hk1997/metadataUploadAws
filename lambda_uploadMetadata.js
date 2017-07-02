'use strict';
var AWS=require('aws-sdk');
var s3=new AWS.S3();
 var dynamoDb=new AWS.DynamoDB();

var allKeys = [];
var s3=new AWS.S3();
console.log('Loading function');

exports.handler = (event, context, callback) => {
    //defining function to get keys from all pages, data upload operation in callback function
    var bucket=event.Records[0].s3.bucket.name; //name of bucket
    var key=event.Records[0].s3.object.key;
    var table='table4';//mention the table name
    var params = {
   Bucket: bucket, 
     Key:key
  };
      s3.headObject(params,function(err,data)
  {
  if(err)
    console.log(err);
  else
    {
      var index={};
        index['objectName']={S:key}; //adding object name
        var s=key;
        s=s.substring(s.length-17,s.length-4);
        var timestamp=new Date(parseInt(s));
        //console.log(timestamp);        
        index['timestamp']={S:timestamp.toGMTString()};
        Object.keys(data.Metadata).forEach(key1=>{
        if(data.Metadata[key1]!=null && !data.Metadata[key1]=='' )
        index[key1]={S:data.Metadata[key1]}
        
      })
     //console.log(index);

      var params_db={
          Item:index,
          TableName:table};
        
               //function to put objects in the dynamoDb
        
        dynamoDb.putItem(params_db,function(err,item)
        {
            if(err)
              {
                console.log(key+" cannot be uploaded due to "+err);
                throw err;
              }
              else
                console.log('item uploaded successfully')
        });
    }
  });    
    callback(null,'finished execution');
    
};


