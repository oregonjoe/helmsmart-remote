//var WebSocketServer = require("ws").Server
var http = require("http")
var express = require("express")
var url = require('url');
const path = require('path');
const querystring = require('querystring');
var app = express()
var port = process.env.PORT || 5000
 
   
const admin = require('firebase-admin');


const { Client } = require('pg');
const pool = require('pg-pool');
const format = require('pg-format');

// make sure to add env variables to the .env file
require("dotenv").config();
const { InfluxDBClient, HttpError, Point } = require("@influxdata/influxdb3-client");
//const { ClientOptions, InfluxDB, Point } = require('@influxdata/influxdb-client');
//import {ClientOptions, InfluxDB, Point} from '@influxdata/influxdb-client'

//const Influx = require('influx');
//const { OrgsAPI, BucketsAPI } = require("@influxdata/influxdb-client-apis");

//import { InfluxDB, Point } from '@influxdata/influxdb-client'
//const { InfluxDBClient, Point } = require("@influxdata/influxdb-client");

//const got = require("got");
//var got = require("got");

// organizationName specifies your InfluxDB organization.
// Organizations are used by InfluxDB to group resources such as users,
// tasks, buckets, dashboards and more.

// your org name, org id, and url can be found at https://cloud2.influxdata.com/me/about
const organizationName = process.env.InfluxDBCloudOrg;
const organizationID = process.env.InfluxDBCloudOrg;

// url is the URL of your InfluxDB instance or Cloud environment.
// This is also the URL where you reach the UI for your account.
const ifdburl = process.env.InfluxDBCloudURL;

// token appropriately scoped to access the resources needed by your app.
// For ease of use in this example, you should use an "all access" token.
// In a production application, you should use a properly scoped token to
// access only the resources needed by your application and store it securely.
// More information about permissions and tokens can be found here:
// https://docs.influxdata.com/influxdb/v2.1/security/tokens/
const token = process.env.InfluxDBCloudToken;

// bucketName specifies an InfluxDB bucket in your organization.
// A bucket is where you store data, and you can group related data into a bucket.
// You can also scope permissions to the bucket level as well.
//const bucketName = process.env.InfluxDBCloudBucket;
const bucketName = "pushsmart-cloud";
//const database = "PushSmart_TCP";
const database = "pushsmart-live";
// client for accessing InfluxDB
//const client = new InfluxDBClient({ url, token, database });
//const client = new InfluxDBClient({ url, token, database });
//const writeAPI = client.getWriteApi(organizationName, bucketName);
//const queryClient = client.getQueryApi(organizationName);


//console.log(url);
//console.log(token);
//console.log(bucketName);


const ifdbclient = new InfluxDBClient({
  host: ifdburl,
  token: token,
  database: bucketName,
});


//const queryApi = new InfluxDB({ifdburl, token}).getQueryApi(organizationName)

//const queryApi = ifdbclient.getQueryApi(organizationName);


/*
const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

client.connect();
*/
const username = "helmsmart";
const password = "Salm0n16";
/*
const retentionPolicy = 'autogen'

const bucket = `${database}/${retentionPolicy}`



const clientOptions: ClientOptions = {
  url: 'http://hilldale-670d9ee3.influxcloud.net:8086',
  token: `${username}:${password}`,
}

const influxDB = new InfluxDB(clientOptions)
*/

/*
const influx = new Influx.InfluxDB({
  host: 'hilldale-670d9ee3.influxcloud.net',
  port: 8086,
  database: database,
  username: username,
  password: password
});
*/

var g_deviceid = "";

var psdata ="initialized";


var hello_message = {
      "endpoints": {
          "v1": {
              "version": "1.1.2",
              "signalk-http": "http://www.helmsmart-remote.com/signalk/v1/api/",
              //"signalk-ws": "ws://stark-beyond-17830-d2fffba06f0f.herokuapp.com/signalk/v1/stream"
          }
      }
  }	
 
//const { InfluxDB, HttpError, Point } = require("@influxdata/influxdb-client");
   
// make sure to add env variables to the .env file
require("dotenv").config();


var privatekey = process.env.private_key;
//console.log(privatekey);

//console.log(privatekey.replace(/\\n/g, '\n'));

var config = {
  "type": "service_account",
  "project_id": process.env.project_id,
  "private_key_id": process.env.private_key_id,
  "private_key": privatekey.replace(/\\n/g, '\n'),
  "client_email": process.env.client_email,
  "client_id": process.env.client_id,
  "auth_uri": process.env.auth_uri,
  "token_uri": process.env.token_uri,
  "auth_provider_x509_cert_url": process.env.auth_provider_x509_cert_url,
  "client_x509_cert_url": process.env.client_x509_cert_url,
  "universe_domain": process.env.universe_domain
}



  
  admin.initializeApp({
  credential: admin.credential.cert(config),
  databaseURL:"https://helmsmart-ios-pcdin.firebaseio.com"
});
  


const db = admin.database();


const clients = new Set();

const connectionPaths = new Map(); // Use a Map for better key handling


process.on('uncaughtException', (err) => {
   console.log(err);
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});

app.use(express.static(__dirname + "/"))

//var server = http.createServer(app)
//server.listen(port)
//console.log("http server listening on %d", port)

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/watch_configs', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'watch_configs.html'));
});

app.get('/download', (req, res) => {
  const file = path.join(__dirname, 'public', 'watch/Dashboard-Config-DI5.Watch.json');
  res.download(file, 'Dashboard-Config-DI5.Watch.json', (err) => {
    if (err) {
      // Handle error, for example file not found.
      res.status(404).send('File not found');
    }
  });
});


app.use('/download', express.static(path.join(__dirname, './watch')));

app.get('/watch', async (req, res) => {
	
	let devicekey = null;
	
	let iwresponse = [];
	let deviceids = [];
	
	const parsedUrl = url.parse(req.url, true);
   //console.log("parsedUrl %s", parsedUrl);
   
   const queryObject = url.parse(req.url,true).query;
		
   //console.log("queryObject %s", queryObject);	
		
		
	try{
		
		devicekey = await queryObject.devicekey.toLowerCase();
		//console.log("devicekey %s", devicekey);
		
	 } catch (error) {
		 
		 //console.log("helmsmart-remote - error in devicekey");
		 res.end("helmsmart-remote - error in devicekey");
	 }
	 
	 
		try{
		
		idkeys = await queryObject.id.toLowerCase();
		console.log("idkey %s", idkeys);
		
	 } catch (error) {
		 
		 console.log("helmsmart-remote - error in idkeys");
		 res.end("helmsmart-remote - error in idkeys");
	 } 
	 
	 
	 
	if(devicekey != null) 
	{
		deviceids = await getedeviceid(devicekey, res)		  
		console.log("iwatch deviceid %s", deviceids);
		
		if(deviceids.length != ""){
			
			
			//measurement = 'HS_' + deviceid.toString(); 
			
			querykeys = idkeys.split(",");
			
			for(var i=0; i < querykeys.length; i++) 
			{
				
				
				console.log("iwatch querykeys %s", querykeys[i]);
				
				let parsedkeys = await parse_idkey(deviceids, querykeys[i]);
				
			
				console.log("iwatch value %s serieskeys %s units %s", parsedkeys.value, parsedkeys.serieskeys, parsedkeys.units);
				
				if (parsedkeys.value != "" && parsedkeys.serieskeys != "")
				{
					
					// query = ('select  percentile({}, 50) AS value from {} ' 'where {} AND time > {}s and time < {}s ') \ .format( value, measurement, serieskeys, startepoch, endepoch)
					//var query = "select *  from 'HS_552376721143_psraw' where  time >= now() - interval '1 minute' order by time desc LIMIT 250";
					//var query = format("select  percentile(%L, 50) AS value from %I where %I and time >= now() - interval '1 minute'", parsedkeys.value, measurement, parsedkeys.serieskeys);
					//var query ="select  percentile(speed, 50) AS value from HS_552376721143 where  deviceid='552376721143' AND  (sensor='engine_parameters_rapid_update') AND  (instance='0')  and time >= now() - interval '1 minute'";
					//var query ="select percentile(speed, 50) AS value from HS_552376721143 where  deviceid='552376721143' AND  (sensor='engine_parameters_rapid_update') AND  (instance='0')   AND time > 1743104390s and time < 1743104510s";
					//var query ="select median(engine_temp) AS  engine_temp from 'HS_552376721143' where  deviceid='552376721143' and sensor='engine_parameters_dynamic' and instance='0' and time >= now() - interval '5 minutes'"
					//var query ="select median(engine_temp) AS  value from 'HS_552376721143' where  deviceid='552376721143' and sensor='engine_parameters_dynamic' and instance='0' and time >= now() - interval '5 minutes'"
					//	var query = "select  median(${parsedkeys.value}) AS value from '${measurement}' where ${parsedkeys.serieskeys} and time >= now() - interval '5 minutes'";
					//	select  median('engine_temp') AS value from "HS_552376721143" where " deviceid='552376721143' AND  (sensor='engine_parameters_dynamic') AND  (instance='0') " and time >= now() - interval '5 minutes'
						measurement = 'HS_' + parsedkeys.deviceid.toString();
						
						var query = format("select  median(%s) AS value from '%s' where %s and time >= now() - interval '5 minutes'", parsedkeys.value, measurement, parsedkeys.serieskeys);	
						console.log("helmsmart-remote influxdb server query %s", query)

							try {

								let queryValue=65535;
							
								const queryResult = await ifdbclient.query(query, database)
								

								for await (const row of queryResult) {
									console.log(`value is ${row.value}`)
									queryValue = row.value;
									//queryValue = parseInt(row.value);
									
								}
								
								/*
										[{"id":"gps.cog","value":44.266200000000005,"unit":"°"},{"id":"gps.latitude","value":42.0504016},{"id":"gps.longitude","value":-124.2683416},{"id":"battery.0.voltage","value":13.33,"unit":"V"}]
								*/
								if(parsedkeys.units == "")
									psdata = {'id': querykeys[i], 'value': queryValue};
									//psdata = {"id":"gps.latitude","value":55.55};
								
								else
									psdata = {'id': querykeys[i], 'value': queryValue, 'unit':parsedkeys.units};
								
								
								
								iwresponse.push(psdata);				
						
							} catch (error) {
								console.error("helmsmart-remote influxdb server writing data:", error);
							  } finally {
								  
								//await ifdbclient.close();
								
								//psdata = {'id': querykeys[i], 'value': queryValue, 'unit':parsedkeys.units};
								//iwresponse.push(psdata);	
								
								
							  }
							
		
					
				} // if the query keys are not null
				
			} // for each query key
		
			//ifdbclient.close()
		
		/*
			//var pushsmart_data = await getData(res, deviceid, "/PCDIN");
			psdata = {'id': 'Engine.0.RPM', 'value': 1234.0, 'unit':'1/min'};
			iwresponse.push(psdata);
			psdata = {'id': 'Engine.0.engineTemperature', 'value': 278.0, 'unit':'°K'};
			iwresponse.push(psdata);
			psdata = {'id': 'Engine.0.oilPressure', 'value': 55, 'unit':'kPa'};
			iwresponse.push(psdata);
			psdata = {'id': 'Engine.0.fuelRate', 'value': 278.0, 'unit':'l/h'};
			iwresponse.push(psdata);			
			*/
			
			console.log("helmsmart-remote iwatch got data:%s length:%s", JSON.stringify(iwresponse), JSON.stringify(iwresponse).length);
			res.send(JSON.stringify(iwresponse));
			res.end();
			
		}
		else{
			
			res.end("helmsmart-remote - no data available");
		}
	}
  
});



app.get('/pushsmart', async (req, res) => {
	
	let devicekey = null;
	let deviceids = [];
	
	const parsedUrl = url.parse(req.url, true);
   //console.log("parsedUrl %s", parsedUrl);
   
   const queryObject = url.parse(req.url,true).query;
		
	try{
		
		devicekey = await queryObject.devicekey.toLowerCase();
		//console.log("devicekey %s", devicekey);
		
	 } catch (error) {
		 
		 //console.log("helmsmart-remote - error in devicekey");
		 res.end("helmsmart-remote - error in devicekey");
	 }
	 
	if(devicekey != null) 
	{
		deviceids = await getedeviceid(devicekey, res)		  
		console.log("deviceids %s", deviceids);
		
		if(deviceids.length != ""){
		
			var pushsmart_data = await getData(res, deviceids[0], "/PCDIN");	
		}
		else{
			
			res.end("helmsmart-remote - no data available");
		}
	}
  
});


app.get('/signalk', async (req, res) => {
	
	let devicekey = null;
	let deviceids = [];
		
		
	const parsedUrl = url.parse(req.url, true);
    console.log("parsedUrl %s", parsedUrl);
   
   const queryObject = url.parse(req.url,true).query;
   //console.log(queryObject);	  
	//deviceid = queryObject.devicekey;
	try{
		
		devicekey = await queryObject.devicekey.toLowerCase();
		//console.log("devicekey %s", devicekey);
		
	 } catch (error) {
		 
		 //console.log("helmsmart-remote - error in devicekey");
		 res.end("helmsmart-remote - error in devicekey");
	 }
			
	if(devicekey != null) 
	{			
		deviceids = await getedeviceid(devicekey, res)		  
		//console.log("deviceid %s", deviceid);
		
		if(deviceids.length != ""){
		
			var pushsmart_data = await getData(res, deviceids[0], "/SIGNALK");	
		}
		else{
			
			res.end("helmsmart-remote - no data available");
		}
	}
	
});


app.get('/json', async (req, res) => {
	
	let devicekey = null;
	let deviceids = [];
		
		
	const parsedUrl = url.parse(req.url, true);
   //console.log("parsedUrl %s", parsedUrl);
   
   const queryObject = url.parse(req.url,true).query;
			  
	try{
		
		devicekey = await queryObject.devicekey.toLowerCase();
		//console.log("devicekey %s", devicekey);
		
	 } catch (error) {
		 
		 //console.log("helmsmart-remote - error in devicekey");
		 res.end("helmsmart-remote - error in devicekey");
	 }
	
	if(devicekey != null) 
	{	
		deviceids = await getedeviceid(devicekey, res)		  
		//console.log("deviceid %s", deviceid);
		
		if(deviceids.length != ""){
		
			var pushsmart_data = await getData(res, deviceids[0], "/JSON");	
		}
		else{
			
			res.end("helmsmart-remote - no data available");
			
		}
	}
	//res.writeHead(200, { 'Content-Type': 'text/text' });
	//res.end(pushsmart_data);
	//res.send(pushsmart_data);
	//res.end();
  
});


// Using async/await
async function getData(res, deviceid, source){
	
	var FireBaseURL = "DEVICE/" + deviceid + source;
	
	
	//console.log("helmsmart-remote firebase urla:%s ", FireBaseURL);
	
//psdata = "helmsmart-remote waiting ...."
	
    try {
        const snapshot = await db.ref(FireBaseURL).get('value');
        if (snapshot.exists()) {
            const psdata = snapshot.val();
            //console.log("Data:", psdata);
			console.log("helmsmart-remote firebase got data:%s length:%s", FireBaseURL, JSON.stringify(psdata).length);
			res.send(psdata);
			res.end();
			
        } else {
            
			psdata = "helmsmart-remote no data available";
			console.log("helmsmart-remote firebase url %s no data available", FireBaseURL);
			
			res.send(psdata);
			res.end();
        }
    } catch (error) {
			//console.error("Error fetching data:", error);
		    console.error('helmsmart-remote firebase url %s error fetching data:%s', FireBaseURL, error);
			psdata = "helmsmart-remote error getting data";
	
	
			switch (error.code) {
			  case 'permission_denied':
				// Handle permission issues
				console.warn('helmsmart-remote firebase Permission denied to access data.');
				break;
			  case 'unavailable':
				// Handle network or service issues
				console.error('helmsmart-remote firebase Network error or service unavailable.');
				break;
			  // Handle other error codes as needed
			  default:
				console.error('helmsmart-remote firebase An unexpected error occurred:', error.message);
			}
	
	
			res.send(psdata);
			res.end();
    }
	
	return psdata;
}


function get_ps_raw(deviceid, source){
	
	
	var FireBaseURL = "DEVICE/" + deviceid + source;
	
	/*

	// Read data from Firebase Realtime Database
	db.ref(FireBaseURL).once('value').then((snapshot) => {
	  psdata = snapshot.val();
	  
	

	  
	});
	*/
	 //psdata = "helmsmart-remote waiting ...."
	// Read data from Firebase Realtime Database
	//db.ref(FireBaseURL).once('value').then(snapshot => {
	db.ref(FireBaseURL).once('value').then(snapshot => {
		
		if (snapshot.exists()) {
		// Handle successful data retrieval
			psdata = snapshot.val();
			
			console.log("helmsmart-remote firebase got data:%s length:%s", FireBaseURL, JSON.stringify(psdata).length);
		} else {
			
			psdata = "helmsmart-remote no data available";
			console.log("helmsmart-remote firebase url %s no data available", FireBaseURL);
		
		}
	
  })
  .catch(error => {
    // Handle errors
    console.error('helmsmart-remote firebase url %s error fetching data:%s', FireBaseURL, error);
	psdata = "helmsmart-remote error getting data";
	
    switch (error.code) {
      case 'permission_denied':
        // Handle permission issues
        console.warn('helmsmart-remote firebase Permission denied to access data.');
        break;
      case 'unavailable':
        // Handle network or service issues
        console.error('helmsmart-remote firebase Network error or service unavailable.');
        break;
      // Handle other error codes as needed
      default:
        console.error('helmsmart-remote firebase An unexpected error occurred:', error.message);
    }
  });
	  
	
	return psdata;
	
	
}
 	
/*
async function getedeviceid(devicekey) {
	
  const client = new Client({
	  connectionString: process.env.DATABASE_URL,
	  ssl: {
		rejectUnauthorized: false
	  }
	});

	//client.connect();
	
	var deviceapikey = "cd7ade4354448b169463652859657cd7";
	
	const querystr = format('select deviceid from user_devices where deviceapikey = %L', deviceapikey);
	console.log("getedeviceid querystr %s", querystr);
	
  
  try {
    await client.connect();
	
    const res = await client.query(querystr);
	
    console.log(res.rows);
	
	  for (let row of res.rows) {
		console.log(JSON.stringify(row));
		
		console.log("getedeviceid deviceid %s", row.deviceid);
	  }
	
  } catch (err) {
	  
    console.error('Error executing query', err);
	
  } finally {
	  
    await client.end();
	
  }
  
  return devicekey;
  
}
*/


async function getedeviceid(deviceapikey, res)  {
	
	var deviceids = [];
	//console.log("getedeviceid start");
	
	
		const client = new Client({
	  connectionString: process.env.DATABASE_URL,
	  ssl: {
		rejectUnauthorized: false
	  }
	});

	
	
  try {
    await client.connect();

	
	const querystr = await format('select deviceid, device_list, devicestatus from user_devices where deviceapikey = %L', deviceapikey.toLowerCase());
	console.log("getedeviceid querystr %s", querystr);
	
    const results = await client.query(querystr);
    console.log(results.rows[0]);
	if (results.rowCount == 0) {
		
		await client.end();
		console.log("not a valid deviceapikey");	
	
		res.end("not a valid deviceapikey");
		return deviceids;
		
	}
	
	
	let row = results.rows[0];
	//deviceid = row.deviceid;
	//var devicestatus = row.devicestatus;
	//console.log("getedeviceid - returning deviceid %s", devicekey);
	if (row.devicestatus == 0) {
		
		await client.end();
		console.log("devicestatus is not active - %s", deviceid);	
	
		res.end("device is not active");
		return deviceids;
		
	}
	
	
	//deviceid: '552376721143',
	// device_list: '552376721143,AC1518EF43B4,001EC010AD69'
	if(row.device_list !== null)
	{
		deviceidkeys = row.device_list.split(",");
		if (deviceidkeys.length == 0)
		{
			deviceids.push(row.deviceid);
		}
		else
		{
			for(i=0; i<deviceidkeys.length; i++)
			{
				
				deviceids.push(deviceidkeys[i]);
			}
			
		}
	}
	else
	{
		deviceids.push(row.deviceid);
		console.log("getedeviceid device_list is empty - %s", deviceids);	
	}	
	
	console.log("getedeviceid deviceids - %s", deviceids);	
	
	
	const updatestr = await format('update user_devices set api_queries = api_queries + 1 where deviceapikey = %L', deviceapikey);
	//console.log("getedeviceid updatestr %s", updatestr);
	
    const updresults = await client.query(updatestr);	
	//console.log(updresults);
	
    await client.end();
	//console.log("await end");
	
	return deviceids;
	
  } catch (err) {
    console.error("Error executing query:", err);
	await client.end();
	res.end("error getting deviceid from deviceapikey");
	
  } finally {
	  
	await client.end();
	return deviceids;
  }
  
   //console.log("await return");
}
/*
// units
    "m",
    "ft",
    "fm",
    "in",
    "mm",
    "km",
    "NM",
    "mile",
    "m/s",
    "km/h",
    "kts",
    "mph",
    "bft",
    "°/min",
    "1/min",
    "Hz",
    "s/sm",
    "m²",
    "m³",
    "l",
    "gal",
    "°C",
    "°F",
    "°K",
    "°",
    "rad",
    "Pa",
    "bar",
    "mbar",
    "kPa",
    "hPa",
    "psi",
    "%",
    "sec",
    "min",
    "h",
    "d",
    "kg/m³",
    "g/m³",
    "ppt",
    "N",
    "kN",
    "kgf",
    "tf",
    "lbf",
    "Nm",
    "g",
    "kg",
    "t",
    "lb",
    "m³/s",
    "l/s",
    "l/h",
    "gph",
    "m/l",
    "km/l",
    "NM/l",
    "l/100km",
    "mpg",
    "NM/g",
    "A",
    "V",
    "W",
    "kW",
    "Ah",
    "mm/h",
    "in/h",

*/	
	
async function parse_idkey(deviceids, idkey) 		
{
	//
	// make sure all the CASE tags are lowercase
	//
	
	
	let value="";
	let serieskeys="";
	let units="";
	let instance = "0";
	
	const instance_indexs = ['port', 'stbd', 'cntr', 'fwd', 'aft'];
	//const index = instance_indexs.indexOf('port'); // Returns 1
	
	//deviceid.552376721143.engine.0.enginetemperature
	//engine.0.enginetemperature
	
	//method=GET path="/watch?devicekey=74d45093bfb70b5623f6bfff8795c152&id=Engine.0.AlternatorVolts,Tank.0.level,Engine.0.EngineTemperature,Engine.0.fuelRate"
	
	
	let keyparts = idkey.split(".");
	
	if( keyparts[0] == "deviceid")
	{
		deviceid = keyparts[1];
		idkey = keyparts.slice(2).join(".");
	}
	else if( keyparts[0] == "deviceindex")
	{
		deviceindex = keyparts[1];
		deviceid=deviceids[deviceindex];
		
		idkey = keyparts.slice(2).join(".");

	}
	else
	{
		deviceid=deviceids[0];
		
	}
	
	console.log("helmsmart-remote - parse_idkey deviceids %s", deviceid);
	console.log("helmsmart-remote - parse_idkey idkey %s", idkey);
	
	let idparts = idkey.split(".");
	
	    if(idparts[0] == "engine") 
		{
			
			switch (idparts[2])
			{
				

				case "rpm":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=speed,sensor=engine_parameters_rapid_update,source=0,type=NULL speed=0 1743367161383000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_rapid_update', 'instance': '0', 'type': 'NULL', 'parameter': 'speed'}:  
				//fields {'speed': 0.0, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='engine_parameters_rapid_update' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "speed";
					units = "1/min";
				break;

				case "enginetemperature":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='engine_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "engine_temp";
					units = "°C";
					units = "°K";
			 
				break;
				
				case "exhausttemperature":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='EGT' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				

				
				case "oilpressure":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=oil_pressure,sensor=engine_parameters_dynamic,source=0,type=NULL oil_pressure=0 1743367161384000000:    
				//tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'oil_pressure'}:  
				//fields {'oil_pressure': 0.0, 'source': 'AA'}: 

					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='engine_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "oil_pressure";
					units = "kPa";
				break;

				case "oiltemperature":

					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='engine_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "oil_temperature";
					units = "°K";
				break;
				
				case "fuelrate":
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'fuel_rate'}:  
				// fields {'fuel_rate': 0.0, 'source': 'AA'}:  
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=fuel_rate,sensor=engine_parameters_dynamic,source=0,type=NULL fuel_rate=0 1743367161384000000: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='engine_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "fuel_rate";
					units = "l/h";
				break;

				case "alternatorvolts":
				//tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'alternator_potential'}:  
				//fields {'alternator_potential': 13.33, 'source': 'AA'}:  
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=alternator_potential,sensor=engine_parameters_dynamic,source=0,type=NULL alternator_potential=13.33 1743367161384000000:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='engine_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "alternator_potential";
					units = "V";
				break;
				
					// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
				
			}// end case			
		}// end if("engine")
		
		else if(idparts[0] == "transmission") 
		{
			
			switch (idparts[2])
			{
				case "oiltemperature":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=voltage,sensor=battery_status,source=0,type=NULL voltage=13.33 1743367161386000000: 		
				// tags {'deviceid': '552376721143', 'sensor': 'battery_status', 'instance': '0', 'type': 'NULL', 'parameter': 'voltage'}:  
				// fields {'voltage': 13.33, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='transmission_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "oil_temp";
					units = "°K";
				break;			
				
				case "oilpressure":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=current,sensor=battery_status,source=0,type=NULL current=0.2 1743367161386000000: 		
				// tags {'deviceid': '552376721143', 'sensor': 'battery_status', 'instance': '0', 'type': 'NULL', 'parameter': 'current'}:  
				// fields {'current': 0.2, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='transmission_parameters_dynamic' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString()  + "' ";
					value = "oil_pressure";
					units = "kPa";
				break;	
				
					// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
				
				
			}// end switch			
		}// end if("transmission")	
	
		else if(idparts[0] == "battery") 
		{
			
			switch (idparts[2])
			{
				case "voltage":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=voltage,sensor=battery_status,source=0,type=NULL voltage=13.33 1743367161386000000: 		
				// tags {'deviceid': '552376721143', 'sensor': 'battery_status', 'instance': '0', 'type': 'NULL', 'parameter': 'voltage'}:  
				// fields {'voltage': 13.33, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='battery_status' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "voltage";
					units = "V";
				break;			
				
				case "current":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=current,sensor=battery_status,source=0,type=NULL current=0.2 1743367161386000000: 		
				// tags {'deviceid': '552376721143', 'sensor': 'battery_status', 'instance': '0', 'type': 'NULL', 'parameter': 'current'}:  
				// fields {'current': 0.2, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='battery_status' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString()  + "' ";
					value = "current";
					units = "A";
				break;	
				
				
				case "temperature":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=current,sensor=battery_status,source=0,type=NULL current=0.2 1743367161386000000: 		
				// tags {'deviceid': '552376721143', 'sensor': 'battery_status', 'instance': '0', 'type': 'NULL', 'parameter': 'current'}:  
				// fields {'current': 0.2, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='battery_status' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString()  + "' ";
					value = "temperature";
					units = "°K";
				break;	
					// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
				
				
				}// end case			
		}// end if("battery")
		
		else if(idparts[0] == "tank") 
		{
			
			switch (idparts[2])
			{
		
				case "level":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=level,sensor=fluid_level,source=0,type=0 level=65 1743367161384000000:				
				// tags {'deviceid': '552376721143', 'sensor': 'fluid_level', 'instance': '0', 'type': '0', 'parameter': 'level'}:  
				// fields {'level': 65.0, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='fluid_level' AND ";
					serieskeys= serieskeys +  " type='0' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "level";
					units = "%";
				break;	
		
		
		
				case "fuellevel":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=level,sensor=fluid_level,source=0,type=0 level=65 1743367161384000000:				
				// tags {'deviceid': '552376721143', 'sensor': 'fluid_level', 'instance': '0', 'type': '0', 'parameter': 'level'}:  
				// fields {'level': 65.0, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='fluid_level' AND ";
					serieskeys= serieskeys +  " type='0' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "level";
					units = "%";
				break;	
				
				case "waterlevel":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=level,sensor=fluid_level,source=0,type=0 level=65 1743367161384000000:				
				// tags {'deviceid': '552376721143', 'sensor': 'fluid_level', 'instance': '0', 'type': '0', 'parameter': 'level'}:  
				// fields {'level': 65.0, 'source': 'AA'}:  
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='fluid_level' AND ";
					serieskeys= serieskeys +  " type='1' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "level";
					units = "%";
				break;	
				
					// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
			}// end case			
		}// end if("tanks")	

		else if(idparts[0] == "temperature") 
		{
			
			switch (idparts[2])
			{
				
				case "sea":
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Sea Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
				break;
				
				case "outside":
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";		 
				break;				
				
				case "engineroom":
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Engine Room Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";	 
				break;					
				
				case "maincabin":
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Main Cabin Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
				break;

				case "livewell":
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Live Well' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
				break;
				
				case "baitwell":
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Bait Well' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
				break;
				
				case "Refrigeration":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Refrigeration' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				
				case "Heating":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Heating' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				
				case "dewpoint":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Dew Point' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				
				case "windchillapparent":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Wind Chill A' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				
				case "windchilltrue":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Wind Chill T' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;

				case "heatindex":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Heat Index' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				
				case "freezer":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Freezer' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;
				
				case "egt":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='EGT' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;	

				case "fuelflow":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=engine_temp,sensor=engine_parameters_dynamic,source=0,type=NULL engine_temp=274.66 1743367161384000000:  
				// tags {'deviceid': '552376721143', 'sensor': 'engine_parameters_dynamic', 'instance': '0', 'type': 'NULL', 'parameter': 'engine_temp'}:  
				// fields {'engine_temp': 274.66, 'source': 'AA'}: 
					//instance = instance_indexs.indexOf(idparts[2]);
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='temperature' AND ";
					serieskeys= serieskeys +  " type='Fuel Flow' AND ";
					serieskeys= serieskeys +  " instance='" + idparts[1].toString() + "' ";
					value = "actual_temperature";
					units = "°K";
			 
				break;	
				
					// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
				
				
			}// end switch			
		}// end if("transmission")	


			
		else if(idparts[0] == "wind") 
		{
			
			switch (idparts[1])
			{
				case "tws":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=wind_speed,sensor=wind_data,source=0,type=TWIND\ True\ North wind_speed=6.32 1743367223516000000: 	
				//tags {'deviceid': '552376721143', 'sensor': 'wind_data', 'instance': '0', 'type': 'TWIND True North', 'parameter': 'wind_speed'}:  
				//fields {'wind_speed': 6.32, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='wind_data' AND ";
					serieskeys= serieskeys +  " type='TWIND True North' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "wind_speed";
					units = "kts";
				break;		
				

				case "twd":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=wind_direction,sensor=wind_data,source=0,type=TWIND\ True\ North wind_direction=156.4821 1743367223516000000:  	
				// tags {'deviceid': '552376721143', 'sensor': 'wind_data', 'instance': '0', 'type': 'TWIND True North', 'parameter': 'wind_direction'}:  
				// fields {'wind_direction': 156.4821, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='wind_data' AND ";
					serieskeys= serieskeys +  " type='TWIND True North' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "wind_direction";
					units = "°";
				break;	
				
				
				case "aws":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=wind_speed,sensor=wind_data,source=0,type=TWIND\ True\ North wind_speed=6.32 1743367223516000000: 	
				//tags {'deviceid': '552376721143', 'sensor': 'wind_data', 'instance': '0', 'type': 'TWIND True North', 'parameter': 'wind_speed'}:  
				//fields {'wind_speed': 6.32, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='wind_data' AND ";
					serieskeys= serieskeys +  " type='Apparent Wind' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "wind_speed";
					units = "kts";
				break;		
				

				case "awa":
				// point HS_552376721143,deviceid=552376721143,instance=0,parameter=wind_direction,sensor=wind_data,source=0,type=TWIND\ True\ North wind_direction=156.4821 1743367223516000000:  	
				// tags {'deviceid': '552376721143', 'sensor': 'wind_data', 'instance': '0', 'type': 'TWIND True North', 'parameter': 'wind_direction'}:  
				// fields {'wind_direction': 156.4821, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='wind_data' AND ";
					serieskeys= serieskeys +  " type='Apparent Wind' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "wind_direction";
					units = "°";
				break;	
				
					// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;

		
				}// end case			
		}// end if("wind")}
		
		else if(idparts[0] == "environment") 
		{
			
			switch (idparts[1])
			{
				case "airtemperature":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=temperature,sensor=environmental_data,source=0,type=Outside\ Temperature temperature=286.45 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'temperature'}:  
				//fields {'temperature': 286.45, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='environmental_data' AND ";
					serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "temperature";
					units = "°K";
				break;		

				case "barometricpressure":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='environmental_data' AND ";
					serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "atmospheric_pressure";
					units = "mbar";
				break;	
				
				case "barometer":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='environmental_data' AND ";
					serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "atmospheric_pressure";
					units = "mbar";
				break;	
				
				case "relativehumidity":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 	
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='environmental_data' AND ";
					serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "humidity";
					units = "%";
				break;				
				
				// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
			}// end case			
		}// end if("environment")
		else if(idparts[0] == "heading") 
		{
			
			switch (idparts[1])
			{
				case "hdg":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=temperature,sensor=environmental_data,source=0,type=Outside\ Temperature temperature=286.45 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'temperature'}:  
				//fields {'temperature': 286.45, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='heading' AND ";
					serieskeys= serieskeys +  " type='true' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "heading";
					units = "°";
				break;		

				case "magneticheading":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='heading' AND ";
					serieskeys= serieskeys +  " type='Magnetic' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "heading";
					units = "°";
				break;	
				
				case "pitch":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='attitude' AND ";
					//serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "pitch";
					units = "°";
				break;	
				
				case "roll":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 	
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='attitude' AND ";
					//serieskeys= serieskeys +  " type='Outside Temperature' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "roll";
					units = "°";
				break;				
				
				// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
			}// end case			
		}// end if("environment")
		else if(idparts[0] == "gps") 
		{
			
			switch (idparts[1])
			{
				case "latitude":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=temperature,sensor=environmental_data,source=0,type=Outside\ Temperature temperature=286.45 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'temperature'}:  
				//fields {'temperature': 286.45, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='position_rapid' AND ";
					serieskeys= serieskeys +  " type='DGNSS fix' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "lat";
					//units = "°";
					units = "";
				break;		

				case "longitude":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='position_rapid' AND ";
					serieskeys= serieskeys +  " type='DGNSS fix' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "lng";
					//units = "°";
					units = "";
				break;	
				
				case "cog":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 		
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='cogsog' AND ";
					serieskeys= serieskeys +  " type='True' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "course_over_ground";
					units = "°";
				break;	
				
				case "sog":
				//point HS_552376721143,deviceid=552376721143,instance=0,parameter=atmospheric_pressure,sensor=environmental_data,source=0,type=Outside\ Temperature atmospheric_pressure=99.9 1743367223516000000: 	
				//tags {'deviceid': '552376721143', 'sensor': 'environmental_data', 'instance': '0', 'type': 'Outside Temperature', 'parameter': 'atmospheric_pressure'}:  
				//fields {'atmospheric_pressure': 99.9, 'source': '24'}:  
					instance = "0";
					serieskeys=" deviceid='";
					serieskeys= serieskeys + deviceid + "' AND ";
					serieskeys= serieskeys +  " sensor='cogsog' AND ";
					serieskeys= serieskeys +  " type='True' AND ";
					serieskeys= serieskeys +  " instance='" + instance + "' ";
					value = "speed_over_ground";
					units = "kts";
				break;				
				
				// If an exact match is not confirmed, this last case will be used if provided
				default:
				break;
			}// end case			
		}// end if("gps")
	
	return {deviceid, value, serieskeys, units} ;
	
	
}
/*	
	
async function getedeviceid(deviceapikey, res) 		
{

	devicekey = "";

console.log("getedeviceid start");

	await client.connect();
	console.log("await connect");

	//var deviceapikey = "cd7ade4354448b169463652859657cd7";
	
	const querystr = await format('select deviceid from user_devices where deviceapikey = %L', deviceapikey);
	console.log("getedeviceid querystr %s", querystr);

	const result = await client.query(querystr);
	console.log("await query");
	     // Release the connection
    await  client.end();
	console.log("await end");
		
	  if (result.rowCount > 0) {
	   console.log("await result");
		  for (let row of result.rows) {
			  
			console.log(JSON.stringify(row));
			devicekey = row.deviceid;
			
		  }
		  
		 
		  console.log(devicekey);
		  return devicekey;
		 
		  
	   }
	  
 console.log("await return");

}

*/



/*
async function getedeviceid(deviceapikey, res) 		
{

	devicekey = "";

	const client = new Client({
	  connectionString: process.env.DATABASE_URL,
	  ssl: {
		rejectUnauthorized: false
	  }
	});

	await client.connect()


	//var deviceapikey = "cd7ade4354448b169463652859657cd7";
	
	const querystr = format('select deviceid from user_devices where deviceapikey = %L', deviceapikey);
	console.log("getedeviceid querystr %s", querystr);


	await client.query(querystr, (err, result) => {
	  //if (err) throw err;
	  
	  if (err) {
		  
		  	console.log("got a postgres error");
			console.error(err);	
			 client.end();
			return "";
		  
	  }
	  
	   if (result.rowCount > 0) {
	  
		  for (let row of result.rows) {
			  
			console.log(JSON.stringify(row));
			devicekey = row.deviceid;
			
		  }
		  
		  return devicekey;
		  client.end();
		  
	   }
	   
	   
	  client.end();
	});

	await client.end();
	
	console.log("getedeviceid - returning deviceid %s", devicekey);
	return devicekey;

}
*/
	
/*
function getedeviceid(devicekey)		
{
	
	  const client = new Client({
	  connectionString: process.env.DATABASE_URL,
	  ssl: {
		rejectUnauthorized: false
	  }
	});

	client.connect();
	
	
	//cursor.execute("select deviceid from user_devices where deviceapikey = %s" , (deviceapikey,))
			
	var deviceapikey = "cd7ade4354448b169463652859657cd7";
	
	const querystr = format('select deviceid from user_devices where deviceapikey = %L', deviceapikey);
	console.log("getedeviceid querystr %s", querystr);
	
	
	client.query(querystr, (err, res) => {
		
	//if (err) throw err;
	
		if (err){
			
			console.log("got a postgres error");
			console.error(err);	
			client.end();
			return "";
		}
		else{
			
			for (let row of res.rows) {
				
				console.log(JSON.stringify(row));
			
				console.log("getedeviceid deviceid %s", row.deviceid);
			}
		
			client.end();
			return devicekey;
			
			//return row.deviceid;
		
	  }
	  
	  
	  //client.end();
	  
	});
	 
	 client.end();
	return devicekey;
	
}
*/

/*
//getedeviceid(devicekey)
//checkRegister: function (req, res) {
function getedeviceid(devicekey, res) {
	
	var deviceid="1234";
	
	
	const pool = new Client({
	  connectionString: process.env.DATABASE_URL,
	  ssl: {
		rejectUnauthorized: false
	  }
	});
	
	
    pool.connect(function (err, client, done) {
        if (err) {
            console.error(err);
            // should return response error like 
            return res.status(500).send();
        }
			
		//var emailCheck = "SELECT id from public.user WHERE email=$1";
		var deviceapikey = "cd7ade4354448b169463652859657cd7";
	
		const querystr = format('select deviceid from user_devices where deviceapikey = %L', deviceapikey);
		console.log("getedeviceid querystr %s", querystr);
	
        client.query(querystr, function (err, result) {
            if (err) {
                console.error(err);
                res.status(500).send();
                return done(); // always close connection
            }
			console.log("getedeviceid result %s", result);
			
            if (result.rowCount > 0) {
                //let deviceid = result.rows[0].deviceid;
                // return your user
                //return done(); // always close connection
				
				for (let row of result.rows) {
				
					console.log(JSON.stringify(row));
				
					console.log("getedeviceid deviceid %s", row.deviceid);
					
					deviceid = row.deviceid;
					
					return done(null, deviceid);
				
				}
				
            } 
			
			client.end();
			return done(null, deviceid);
			
        })
    })
    pool.on('error', function (err, client) {
        console.error('idle client error', err.message, err.stack)
    });
} 

*/
