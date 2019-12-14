from influxdb import InfluxDBClient

loginEvents = [{"measurement":"UserLogins",
        "tags": {
            "Area": "North America",
            "Location": "New York City",
            "ClientIP": "192.168.0.256"
        },
        "fields":
        {
        "SessionDuration":1.2
        }       
        },
        {"measurement":"UserLogins",
          "tags": {
            "Area": "South America-popo",
            "Location": "Lima",
            "ClientIP": "192.168.1.256"
        },
        "fields":
        {
        "SessionDuration":2.0
        }       
    }        
]

dbClient = InfluxDBClient('qwerty.com.ar', 8086, 'mim_tp1', 'mim_tp1_transporte', 'AccessHistory')

# Write the time series data points into database - user login details
dbClient.create_database('AccessHistory')
dbClient.write_points(loginEvents)

# Query the IPs from logins have been made
loginRecords = dbClient.query('select * from UserLogins;')

# Print the time series query results
print(loginRecords)