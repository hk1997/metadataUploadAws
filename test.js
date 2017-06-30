var AWS=require('aws-sdk');

AWS.config.update({
  accessKeyId: '',
  secretAccessKey: '',
  region: ''
});

var s3=new AWS.S3();
 var dynamoDb=new AWS.DynamoDB();

var allKeys = [];


const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

rl.question('Please enter the bucket name : ', (bucket) => {
    rl.question('Please enter the table name : ', (table) => {
        
        rl.close();
//defining function to get keys from all pages, data upload operation in callback function
function listAllKeys(marker, cb)
{
  s3.listObjects({Bucket:bucket, Marker: marker}, function(err, data){
    allKeys.push(data.Contents);

    
      console.log('all keys have been listed now processing them to upload data in '+table);
    allKeys[0].forEach(i=>{
    console.log(i.Key);
  
  var params = {
   Bucket: bucket, 
     Key:i.Key
  };
      s3.headObject(params,function(err,data)
  {
  if(err)
    console.log(err);
  else
    {
      var index={};
        index['objectName']={S:i.Key}; //adding object name
        var s=i.Key;
        s=s.substring(s.length-17,s.length-4);
        var timestamp=new Date(parseInt(s));
        //console.log(timestamp);        
        index['timestamp']={S:timestamp.toGMTString()};
        Object.keys(data.Metadata).forEach(key=>{
        if(data.Metadata[key]!=null && !data.Metadata[key]=='' )
        index[key]={S:data.Metadata[key]}
    
      })
      console.log(index);

      var params_db={
          Item:index,
          TableName:table};
        
               //function to put objects in the dynamoDb
        
        dynamoDb.putItem(params_db,function(err,item)
        {
            if(err)
              {
                console.log(i.Key+" cannot be uploaded due to "+err);
                throw err;
              }
            else
            console.log("upload of item successful "+i.Key);
        });


    }
})
  });






    if(data.IsTruncated)
    {
      allKeys=[];
      listAllKeys(data.NextMarker, cb);
    }
    else
      cb();
  });
}

//calling the function now
listAllKeys('',function(){
  console.log('wait for asynchronous calls to end');
})


/*//calling list all keys function
listAllKeys('',function()
{	//callback function 
	//console.log('all keys have been listed now processing them to upload data in '+table);
		allKeys[0].forEach(i=>{
		console.log(i.Key);
	
	var params = {
 	 Bucket: bucket, 
 		 Key:i.Key
 	};
			s3.headObject(params,function(err,data)
	{
	if(err)
		console.log(err);
	else
		{
			var index={};
			  index['objectName']={S:i.Key}; //adding object name
        var s=i.Key;
        s=s.substring(s.length-17,s.length-4);
        var timestamp=new Date(parseInt(s));
        //console.log(timestamp);        
        index['timestamp']={S:timestamp.toGMTString()};
				Object.keys(data.Metadata).forEach(key=>{
				if(data.Metadata[key]!=null && !data.Metadata[key]=='' )
				index[key]={S:data.Metadata[key]}
		
			})
			console.log(index);

			var params_db={
        	Item:index,
	        TableName:table};
        
               //function to put objects in the dynamoDb
        
        dynamoDb.putItem(params_db,function(err,item)
        {
            if(err)
            	{
            		console.log(i.Key+" cannot be uploaded due to "+err);
            		throw err;
            	}
            else
            console.log("upload of item successful "+i.Key);
        });


		}
})
	});


});
*/


    });
});
