const pg = require("pg");
const msRestAzure = require("ms-rest-azure");
const PostgreSQLManagementClient = require('azure-arm-postgresql');
const MonitorManagementClient = require("azure-arm-monitor");
const https = require('https');
const _ = require('lodash');

module.exports = async function (context, myTimer) {
    const monitoredInstances = await findMonitoredAzureDBPostgreSQLInstances();
    for (i = 0; i < monitoredInstances.length; i++) {
        let instanceName = monitoredInstances[i];
        let [availability, metrics] = await Promise.all([getPGAvailabilty(instanceName), fetchMetrics(instanceName)]);
        sendMetrics(instanceName, metrics, availability);
    }
};

async function findMonitoredAzureDBPostgreSQLInstances() {
    console.log("Finding all the instances ..")
    const accountAccessConfig = getAccountSecrets();
    const clientId = accountAccessConfig.clientId;
    const clientSecret = accountAccessConfig.clientSecret;
    const tenantId = accountAccessConfig.tenantId;
    const subscriptionId = accountAccessConfig.subscriptionId;
    const resourceGroupName = accountAccessConfig.resourceGroupName;
    var instanceList = [];
    try {
        let credentials = await msRestAzure.loginWithServicePrincipalSecret(clientId, clientSecret, tenantId);
        let client = new PostgreSQLManagementClient(credentials, subscriptionId);
        let servers = await client.servers.listByResourceGroup(resourceGroupName);
        console.log("Checking instances one by one");
        var i;
        if (servers != null) {
            for (i = 0; i < servers.length; i++) {
                let instanceName = servers[i].name;
                let tagList = servers[i].tags;
                var j;
                if (tagList != null) {
                    let monitoringEnabled = tagList.EnableMonitoring;
                    if (monitoringEnabled != null && monitoringEnabled === "true") {
                        console.log("Monitoring is enabled for ===> " + instanceName);
                        instanceList.push(instanceName);
                    }
                }
            }
        }
    } catch (err) {
        console.log("Error in finding rds instance(s) : " + err);
    }
    return instanceList;
}

async function getPGAvailabilty(instanceName) {

    var dbCreds = process.env[instanceName + '_db_creds'];

    if (!(dbCreds)) {
        console.log("Credentials key not provided, Sending availabilty as 0");
        return 0;
    }

    dbCredentials = JSON.parse(dbCreds)

    const userName = dbCredentials.username;
    const host = dbCredentials.hostname;
    const password = dbCredentials.password;

    if (!(userName && host && password)) {
        console.log("Credentials not provided, Sending availabilty as 0");
        console.log(userName + " == " + host + " == " + password);
        return 0;
    }

    try {
        const client = new pg.Client({
            user: userName,
            password: password,
            host: host,
            port: 5432,
            database: 'azure_db_postgresql',
            ssl: true,
            statement_timeout: 3000
        });
        await client.connect();

        var res = await client.query("SELECT pg_is_in_recovery()");
        var status;

        res.rows.forEach(row => {

            //console.log(row);
            if (row.pg_is_in_recovery == false) {
                status = 1;
            } else {
                status = 0;
            }
        });

        await client.end();
    } catch (err) {
        status = 0;
        console.log("Error in connecting to database.")
    }

    return status;
}
function getAccountSecrets() {
    let config = {};
    _.set(config, 'clientId', process.env['clientId']);
    _.set(config, 'clientSecret', process.env['clientSecret']);
    _.set(config, 'tenantId', process.env['tenantId']);
    _.set(config, 'subscriptionId', process.env['subscriptionId']);
    _.set(config, 'resourceGroupName', process.env['resourceGroupName']);
    return config;
}

async function fetchMetrics(instanceName) {
    const accountAccessConfig = getAccountSecrets();
    const clientId = accountAccessConfig.clientId;
    const clientSecret = accountAccessConfig.clientSecret;
    const tenantId = accountAccessConfig.tenantId;
    const subscriptionId = accountAccessConfig.subscriptionId;
    const resourceGroupName = accountAccessConfig.resourceGroupName;
    const metricNames = 'active_connections,storage_used,cpu_percent';
    const endTime = new Date().toISOString();
    const startTime = new Date(Date.now() - 60000 * 5).toISOString();
    const timespan = `${startTime}/${endTime}`;
    const aggregation = "Average";
    let credentials = await msRestAzure.loginWithServicePrincipalSecret(clientId, clientSecret, tenantId);
    const monitorClient = new MonitorManagementClient(credentials, subscriptionId);
    try {
        const metrics_list = await monitorClient.metrics.list(`subscriptions/${subscriptionId}/resourceGroups/${resourceGroupName}/providers/Microsoft.DBforPostgreSQL/servers/${instanceName}`, { "metricnames": metricNames, "timespan": timespan, "aggregation": aggregation });
        console.log(metrics_list);
        return metrics_list.value;
    } catch (err) {
        console.log(err.stack)
    }
}
function sendMetrics(instanceName, metrics, availability) {

    const dynatrace_token = process.env.dynatrace_token;
    const dynatrace_endpoint = process.env.dynatrace_endpoint;
    const dynatrace_environment_id = process.env.dynatrace_environment_id;

    const token = "Api-Token " + dynatrace_token;
    const apiUrl = dynatrace_endpoint;
    const path = "/e/" + dynatrace_environment_id + "/api/v1/entity/infrastructure/custom";

    var cpu = metrics[2].timeseries[0].data[0].average;
    var connections = metrics[0].timeseries[0].data[0].average;
    var storage_used = metrics[1].timeseries[0].data[0].average / 1024 / 1024 / 1024; //converting bytes to GB
    const currentTime = new Date().getTime();

    var data = {
        "tags": [
            "Azure HyperScalar Instance"
        ],
        "type": "HyperScalar-Azure-Postgresql",
        "properties": {
            "InstanceType": "PostgreSQL on Microsoft Azure"
        },
        "series": [
            {
                "timeseriesId": "custom:postgresql.cpu.utilization",
                "dimensions": {
                    "usedcpu": "currentCPUUsage"
                },
                "dataPoints": [
                    [currentTime, cpu],
                    [currentTime, cpu]
                ]
            },
            {
                "timeseriesId": "custom:postgresql.Instance.DBConnections",
                "dimensions": {
                    "dbconnections": "Current Database Connections"
                },
                "dataPoints": [
                    [currentTime, connections],
                    [currentTime, connections]
                ]
            },
            {
                "timeseriesId": "custom:postgresql.Instance.UsedSpace",
                "dimensions": {
                    "usedstoragespace": "Current Usage of Disk Space"
                },
                "dataPoints": [
                    [currentTime, storage_used],
                    [currentTime, storage_used]
                ]
            },
            {
                "timeseriesId": "custom:postgresql.Instance.Availability",
                "dimensions": {
                    "availability": "Current Availability"
                },
                "dataPoints": [
                    [currentTime, availability],
                    [currentTime, availability]
                ]
            }
        ]
    }

    var postData = JSON.stringify(data);
    console.log(postData);
    var options = {
        hostname: apiUrl,//'apm.cf.stagingazure.hanavlab.ondemand.com',
        path: path + '/' + instanceName,//'/e/b7ecc45a-af75-4947-924d-fa7d62311093/api/v1/entity/infrastructure/custom/postgresql-instance-2',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': token//'Api-Token HauOnMXjT_m_KC5VjrwUa'
        }
    };

    var req = https.request(options, (res) => {
        console.log(`STATUS: ${res.statusCode}`);
        console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
            console.log(`BODY: ${chunk}`);
        });
        res.on('end', () => {
            console.log('No more data in response.');
        });
    });

    req.on('error', (e) => {
        console.log(`problem with request: ${e.message}`);
    });

    // write data to request body
    req.write(postData);
    req.end();
}

