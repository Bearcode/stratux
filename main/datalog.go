/*
	Copyright (c) 2015-2016 Christopher Young
	Distributable under the terms of The "BSD New"" License
	that can be found in the LICENSE file, herein included
	as part of this header.

	datalog.go: Log stratux data as it is received. Bucket data into timestamp time slots.

*/

package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"github.com/kellydunn/golang-geo"
	"github.com/bradfitz/latlong"
)

const (
	LOG_TIMESTAMP_RESOLUTION = 250 * time.Millisecond
	MIN_FLIGHT_SPEED = 35
	MIN_TAXI_SPEED = 10
)

type StratuxTimestamp struct {
	id                   int64
	Time_type_preference int // 0 = stratuxClock, 1 = gpsClock, 2 = gpsClock extrapolated via stratuxClock.
	StratuxClock_value   time.Time
	GPSClock_value       time.Time // The value of this is either from the GPS or extrapolated from the GPS via stratuxClock if pref is 1 or 2. It is time.Time{} if 0.
	PreferredTime_value  time.Time
	StartupID            int64
}

// 'startup' table creates a new entry each time the daemon is started. This keeps track of sequential starts, even if the
//  timestamp is ambiguous (units with no GPS). This struct is just a placeholder for an empty table (other than primary key).
type StratuxStartup struct {
	id         int64
	start_loc  string     // starting location (airport or GPS coordinate)
	start_ts   string 	  // starting timestamp (GPS date/time format)
	duration   int64	  // duration of the flight in minutes
	distance   int64      // distance of flight in nm
	route      string	  // route of flight (list of airport ids / coordinate points)
}

var dataLogStarted bool
var dataLogReadyToWrite bool
var lastSituationLogMs uint64

var minimumFlightSpeed uint16 = MIN_FLIGHT_SPEED
var minimumTaxiSpeed uint16 = MIN_TAXI_SPEED

var stratuxStartupID int64
var dataLogTimestamps []StratuxTimestamp
var dataLogCurTimestamp int64 // Current timestamp bucket. This is an index on dataLogTimestamps which is not necessarily the db id.

/*
	values / flags used by flight logging code (see: logSituation() below)
*/
var lastTimestamp *time.Time
var lastPoint *geo.Point
var weAreFlying bool
var weAreTaxiing bool

/*
	airport structure - used by the airport lookup utility
*/
type airport struct {
	faaId string
	icaoId string
	name string
	lat float64
	lng float64
	alt float64
	dst float64
}

/*
	checkTimestamp().
		Verify that our current timestamp is within the LOG_TIMESTAMP_RESOLUTION bucket.
		 Returns false if the timestamp was changed, true if it is still valid.
		 This is where GPS timestamps are extrapolated, if the GPS data is currently valid.
*/

func checkTimestamp() bool {
	thisCurTimestamp := dataLogCurTimestamp
	if stratuxClock.Since(dataLogTimestamps[thisCurTimestamp].StratuxClock_value) >= LOG_TIMESTAMP_RESOLUTION {
		var ts StratuxTimestamp
		ts.id = 0
		ts.Time_type_preference = 0 // stratuxClock.
		ts.StratuxClock_value = stratuxClock.Time
		ts.GPSClock_value = time.Time{}
		ts.PreferredTime_value = stratuxClock.Time

		// Extrapolate from GPS timestamp, if possible.
		if isGPSClockValid() && thisCurTimestamp > 0 {
			// Was the last timestamp either extrapolated or GPS time?
			last_ts := dataLogTimestamps[thisCurTimestamp]
			if last_ts.Time_type_preference == 1 || last_ts.Time_type_preference == 2 {
				// Extrapolate via stratuxClock.
				timeSinceLastTS := ts.StratuxClock_value.Sub(last_ts.StratuxClock_value) // stratuxClock ticks since last timestamp.
				extrapolatedGPSTimestamp := last_ts.PreferredTime_value.Add(timeSinceLastTS)

				// Re-set the preferred timestamp type to '2' (extrapolated time).
				ts.Time_type_preference = 2
				ts.PreferredTime_value = extrapolatedGPSTimestamp
				ts.GPSClock_value = extrapolatedGPSTimestamp
			}
		}

		dataLogTimestamps = append(dataLogTimestamps, ts)
		dataLogCurTimestamp = int64(len(dataLogTimestamps) - 1)
		return false
	}
	return true
}

type SQLiteMarshal struct {
	FieldType string
	Marshal   func(v reflect.Value) string
}

func boolMarshal(v reflect.Value) string {
	b := v.Bool()
	if b {
		return "1"
	}
	return "0"
}

func intMarshal(v reflect.Value) string {
	return strconv.FormatInt(v.Int(), 10)
}

func uintMarshal(v reflect.Value) string {
	return strconv.FormatUint(v.Uint(), 10)
}

func floatMarshal(v reflect.Value) string {
	return strconv.FormatFloat(v.Float(), 'f', 10, 64)
}

func stringMarshal(v reflect.Value) string {
	return v.String()
}

func notsupportedMarshal(v reflect.Value) string {
	return ""
}

func structCanBeMarshalled(v reflect.Value) bool {
	m := v.MethodByName("String")
	if m.IsValid() && !m.IsNil() {
		return true
	}
	return false
}

func structMarshal(v reflect.Value) string {
	if structCanBeMarshalled(v) {
		m := v.MethodByName("String")
		in := make([]reflect.Value, 0)
		ret := m.Call(in)
		if len(ret) > 0 {
			return ret[0].String()
		}
	}
	return ""
}

var sqliteMarshalFunctions = map[string]SQLiteMarshal{
	"bool":         {FieldType: "INTEGER", Marshal: boolMarshal},
	"int":          {FieldType: "INTEGER", Marshal: intMarshal},
	"uint":         {FieldType: "INTEGER", Marshal: uintMarshal},
	"float":        {FieldType: "REAL", Marshal: floatMarshal},
	"string":       {FieldType: "TEXT", Marshal: stringMarshal},
	"struct":       {FieldType: "STRING", Marshal: structMarshal},
	"notsupported": {FieldType: "notsupported", Marshal: notsupportedMarshal},
}

var sqlTypeMap = map[reflect.Kind]string{
	reflect.Bool:          "bool",
	reflect.Int:           "int",
	reflect.Int8:          "int",
	reflect.Int16:         "int",
	reflect.Int32:         "int",
	reflect.Int64:         "int",
	reflect.Uint:          "uint",
	reflect.Uint8:         "uint",
	reflect.Uint16:        "uint",
	reflect.Uint32:        "uint",
	reflect.Uint64:        "uint",
	reflect.Uintptr:       "notsupported",
	reflect.Float32:       "float",
	reflect.Float64:       "float",
	reflect.Complex64:     "notsupported",
	reflect.Complex128:    "notsupported",
	reflect.Array:         "notsupported",
	reflect.Chan:          "notsupported",
	reflect.Func:          "notsupported",
	reflect.Interface:     "notsupported",
	reflect.Map:           "notsupported",
	reflect.Ptr:           "notsupported",
	reflect.Slice:         "notsupported",
	reflect.String:        "string",
	reflect.Struct:        "struct",
	reflect.UnsafePointer: "notsupported",
}

func makeTable(i interface{}, tbl string, db *sql.DB) {
	val := reflect.ValueOf(i)

	fields := make([]string, 0)
	for i := 0; i < val.NumField(); i++ {
		kind := val.Field(i).Kind()
		fieldName := val.Type().Field(i).Name
		sqlTypeAlias := sqlTypeMap[kind]

		// Check that if the field is a struct that it can be marshalled.
		if sqlTypeAlias == "struct" && !structCanBeMarshalled(val.Field(i)) {
			continue
		}
		if sqlTypeAlias == "notsupported" || fieldName == "id" {
			continue
		}
		sqlType := sqliteMarshalFunctions[sqlTypeAlias].FieldType
		s := fieldName + " " + sqlType
		fields = append(fields, s)
	}

	// Add the timestamp_id field to link up with the timestamp table.
	if tbl != "timestamp" && tbl != "startup" {
		fields = append(fields, "timestamp_id INTEGER")
		fields = append(fields, "startup_id INTEGER")
	}
	
	tblCreate := fmt.Sprintf("CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, %s)", tbl, strings.Join(fields, ", "))

	_, err := db.Exec(tblCreate)
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
	}
}

/*
	bulkInsert().
		Reads insertBatch and insertBatchIfs. This is called after a group of insertData() calls.
*/

func bulkInsert(tbl string, db *sql.DB) (res sql.Result, err error) {
	if _, ok := insertString[tbl]; !ok {
		return nil, errors.New("no insert statement")
	}

	batchVals := insertBatchIfs[tbl]
	numColsPerRow := len(batchVals[0])
	maxRowBatch := int(999 / numColsPerRow) // SQLITE_MAX_VARIABLE_NUMBER = 999.
	//	log.Printf("table %s. %d cols per row. max batch %d\n", tbl, numColsPerRow, maxRowBatch)
	for len(batchVals) > 0 {
		//     timeInit := time.Now()
		i := int(0) // Variable number of rows per INSERT statement.

		stmt := ""
		vals := make([]interface{}, 0)
		querySize := uint64(0)                                            // Size of the query in bytes.
		for len(batchVals) > 0 && i < maxRowBatch && querySize < 750000 { // Maximum of 1,000,000 bytes per query.
			if len(stmt) == 0 { // The first set will be covered by insertString.
				stmt = insertString[tbl]
				querySize += uint64(len(insertString[tbl]))
			} else {
				addStr := ", (" + strings.Join(strings.Split(strings.Repeat("?", len(batchVals[0])), ""), ",") + ")"
				stmt += addStr
				querySize += uint64(len(addStr))
			}
			for _, val := range batchVals[0] {
				querySize += uint64(len(val.(string)))
			}
			vals = append(vals, batchVals[0]...)
			batchVals = batchVals[1:]
			i++
		}
		//		log.Printf("inserting %d rows to %s. querySize=%d\n", i, tbl, querySize)
		res, err = db.Exec(stmt, vals...)
		//      timeBatch := time.Since(timeInit)                                                                                                                     // debug
		//      log.Printf("SQLite: bulkInserted %d rows to %s. Took %f msec to build and insert query. querySize=%d\n", i, tbl, 1000*timeBatch.Seconds(), querySize) // debug
		if err != nil {
			log.Printf("sqlite INSERT error: '%s'\n", err.Error())
			return
		}
	}

	// Clear the buffers.
	delete(insertString, tbl)
	delete(insertBatchIfs, tbl)

	return
}

/*
	insertData().
		Inserts an arbitrary struct into an SQLite table.
		 Inserts the timestamp first, if its 'id' is 0.

*/

// Cached 'VALUES' statements. Indexed by table name.
var insertString map[string]string // INSERT INTO tbl (col1, col2, ...) VALUES(?, ?, ...). Only for one value.
var insertBatchIfs map[string][][]interface{}

func insertData(i interface{}, tbl string, db *sql.DB, ts_num int64) int64 {
	val := reflect.ValueOf(i)

	keys := make([]string, 0)
	values := make([]string, 0)
	for i := 0; i < val.NumField(); i++ {
		kind := val.Field(i).Kind()
		fieldName := val.Type().Field(i).Name
		sqlTypeAlias := sqlTypeMap[kind]

		if sqlTypeAlias == "notsupported" || fieldName == "id" {
			continue
		}

		v := sqliteMarshalFunctions[sqlTypeAlias].Marshal(val.Field(i))

		keys = append(keys, fieldName)
		values = append(values, v)
	}

	// Add the timestamp_id field to link up with the timestamp table.
	if tbl != "timestamp" && tbl != "startup" {
		keys = append(keys, "timestamp_id")
/*
	NOTE: this has been disabled and will be removed. The logging system has been revised
	to simply record the startup_id value and a timestamp_id which is the StratuxClock 
	Millisecond value. (TODO: change that to 'timestamp' instead of 'timestamp_id'). The
	value of timestamp_id is relative to the startup time for the associated startup.
	
	Example: Flight is part of startup 92. The startup record's start_timestamp (Unix 
	timestamp) equates to "2016-09-28 11:22:14.9 +0000 UTC". Each timestamp_id value is
	the number of nanoseconds since the startup (taken from stratuxClock).
	
	To convert a specific timestamp to a time literal, simply add it to the start_timestamp
	value and then convert the Unix timestamp (milliseconds) to UTC or a local date/time.
	
	This change simplifies the process of gathering data associated with a given startup
	and also significantly reduces the number of writes to the database.
	
		if dataLogTimestamps[ts_num].id == 0 {
			//FIXME: This is somewhat convoluted. When insertData() is called for a ts_num that corresponds to a timestamp with no database id,
			// then it inserts that timestamp via the same interface and the id is updated in the structure via the below lines
			// (dataLogTimestamps[ts_num].id = id).
			dataLogTimestamps[ts_num].StartupID = stratuxStartupID
			insertData(dataLogTimestamps[ts_num], "timestamp", db, ts_num) // Updates dataLogTimestamps[ts_num].id.
		}
*/
		// revised: simply uses ms value from stratuxClock now - see above note.
		values = append(values, strconv.FormatInt(int64(stratuxClock.Milliseconds), 10))
		keys = append(keys, "startup_id")
		values = append(values, strconv.FormatInt(stratuxStartupID, 10))
	}

//TODO: create and cache a statement for each table - no reason to re-create each time
	if _, ok := insertString[tbl]; !ok {
		// Prepare the statement.
		tblInsert := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", tbl, strings.Join(keys, ","),
			strings.Join(strings.Split(strings.Repeat("?", len(keys)), ""), ","))
		insertString[tbl] = tblInsert
	}

	// Make the values slice into a slice of interface{}.
	ifs := make([]interface{}, len(values))
	for i := 0; i < len(values); i++ {
		ifs[i] = values[i]
	}

	insertBatchIfs[tbl] = append(insertBatchIfs[tbl], ifs)

	if tbl == "timestamp" || tbl == "startup" { // Immediate insert always for "timestamp" and "startup" table.
		res, err := bulkInsert(tbl, db) // Bulk insert of 1, always.
		if err == nil {
			id, err := res.LastInsertId()
			if err == nil && tbl == "timestamp" { // Special handling for timestamps. Update the timestamp ID.
				ts := dataLogTimestamps[ts_num]
				ts.id = id
				dataLogTimestamps[ts_num] = ts
			}
			return id
		}
	}

	return 0
}

type DataLogRow struct {
	tbl    string
	data   interface{}
	ts_num int64
}

var dataLogChan chan DataLogRow
var shutdownDataLog chan bool
var shutdownDataLogWriter chan bool
var dataUpdateChan chan bool
var dataLogWriteChan chan DataLogRow

func dataLogWriter(db *sql.DB) {
	dataLogWriteChan = make(chan DataLogRow, 10240)
	shutdownDataLogWriter = make(chan bool)
	// The write queue. As data comes in via dataLogChan, it is timestamped and stored.
	//  When writeTicker comes up, the queue is emptied.
	writeTicker := time.NewTicker(10 * time.Second)
	rowsQueuedForWrite := make([]DataLogRow, 0)
	for {
		select {
		case r := <-dataLogWriteChan:
			// Accept timestamped row.
			rowsQueuedForWrite = append(rowsQueuedForWrite, r)
		case <-dataUpdateChan:
			// Start transaction.
			tx, err := db.Begin()
			if err != nil {
				log.Printf("db.Begin() error: %s\n", err.Error())
				break // from select {}
			}
			updateFlightLog(db)
			// Close the transaction.
			tx.Commit()
		case <-writeTicker.C:
			//			for i := 0; i < 1000; i++ {
			//				logSituation()
			//			}
			timeStart := stratuxClock.Time
			nRows := len(rowsQueuedForWrite)
			if globalSettings.DEBUG {
				log.Printf("Writing %d rows\n", nRows)
			}
			// Write the buffered rows. This will block while it is writing.
			// Save the names of the tables affected so that we can run bulkInsert() on after the insertData() calls.
			tblsAffected := make(map[string]bool)
			// Start transaction.
			tx, err := db.Begin()
			if err != nil {
				log.Printf("db.Begin() error: %s\n", err.Error())
				break // from select {}
			}
			for _, r := range rowsQueuedForWrite {
				tblsAffected[r.tbl] = true
				insertData(r.data, r.tbl, db, r.ts_num)
			}
			// Do the bulk inserts.
			for tbl, _ := range tblsAffected {
				bulkInsert(tbl, db)
			}
			// Close the transaction.
			tx.Commit()
			rowsQueuedForWrite = make([]DataLogRow, 0) // Zero the queue.
			timeElapsed := stratuxClock.Since(timeStart)
			if globalSettings.DEBUG {
				rowsPerSecond := float64(nRows) / float64(timeElapsed.Seconds())
				log.Printf("Writing finished. %d rows in %.2f seconds (%.1f rows per second).\n", nRows, float64(timeElapsed.Seconds()), rowsPerSecond)
			}
			if timeElapsed.Seconds() > 10.0 {
				log.Printf("WARNING! SQLite logging is behind. Last write took %.1f seconds.\n", float64(timeElapsed.Seconds()))
				dataLogCriticalErr := fmt.Errorf("WARNING! SQLite logging is behind. Last write took %.1f seconds.\n", float64(timeElapsed.Seconds()))
				addSystemError(dataLogCriticalErr)
			}
		case <-shutdownDataLogWriter: // Received a message on the channel to initiate a graceful shutdown, and to command dataLog() to shut down
			log.Printf("datalog.go: dataLogWriter() received shutdown message with rowsQueuedForWrite = %d\n", len(rowsQueuedForWrite))
			shutdownDataLog <- true
			return
		}
	}
	log.Printf("datalog.go: dataLogWriter() shutting down\n")
}

func dataLog() {
	dataLogStarted = true
	log.Printf("datalog.go: dataLog() started\n")
	dataLogChan = make(chan DataLogRow, 10240)
	shutdownDataLog = make(chan bool)
	dataLogTimestamps = make([]StratuxTimestamp, 0)
	var ts StratuxTimestamp
	ts.id = 0
	ts.Time_type_preference = 0 // stratuxClock.
	ts.StratuxClock_value = stratuxClock.Time
	ts.GPSClock_value = time.Time{}
	ts.PreferredTime_value = stratuxClock.Time
	dataLogTimestamps = append(dataLogTimestamps, ts)
	dataLogCurTimestamp = 0

	// Check if we need to create a new database.
	createDatabase := false

	if _, err := os.Stat(dataLogFilef); os.IsNotExist(err) {
		createDatabase = true
		log.Printf("creating new database '%s'.\n", dataLogFilef)
	}

	db, err := sql.Open("sqlite3", dataLogFilef)
	if err != nil {
		log.Printf("sql.Open(): %s\n", err.Error())
	}

	defer func() {
		db.Close()
		dataLogStarted = false
		//close(dataLogChan)
		log.Printf("datalog.go: dataLog() has closed DB in %s\n", dataLogFilef)
	}()

	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		log.Printf("db.Exec('PRAGMA journal_mode=WAL') err: %s\n", err.Error())
	}
	_, err = db.Exec("PRAGMA synchronous=OFF")
	if err != nil {
		log.Printf("db.Exec('PRAGMA journal_mode=WAL') err: %s\n", err.Error())
	}

	//log.Printf("Starting dataLogWriter\n") // REMOVE -- DEBUG
	go dataLogWriter(db)

	// Do we need to create the database?
	if createDatabase {
		makeTable(StratuxTimestamp{}, "timestamp", db)
		makeTable(mySituation, "mySituation", db)
		makeTable(globalStatus, "status", db)
		makeTable(globalSettings, "settings", db)
		makeTable(TrafficInfo{}, "traffic", db)
		makeTable(msg{}, "messages", db)
		makeTable(esmsg{}, "es_messages", db)
		makeTable(Dump1090TermMessage{}, "dump1090_terminal", db)
		//makeTable(StratuxStartup{}, "startup", db)
		makeTable(FlightLog{}, "startup", db)
	}

	// The first entry to be created is the "startup" entry.
	stratuxStartupID = insertData(FlightLog{}, "startup", db, 0)

	dataLogReadyToWrite = true
	//log.Printf("Entering dataLog read loop\n") //REMOVE -- DEBUG
	for {
		select {
		case r := <-dataLogChan:
			// When data is input, the first step is to timestamp it.
			// Check if our time bucket has expired or has never been entered.
			checkTimestamp()
			// Mark the row with the current timestamp ID, in case it gets entered later.
			r.ts_num = dataLogCurTimestamp
			// Queue it for the scheduled write.
			dataLogWriteChan <- r
		case <-shutdownDataLog: // Received a message on the channel to complete a graceful shutdown (see the 'defer func()...' statement above).
			log.Printf("datalog.go: dataLog() received shutdown message\n")
			return
		}
	}
	log.Printf("datalog.go: dataLog() shutting down\n")
	close(shutdownDataLog)
}

/*
	setDataLogTimeWithGPS().
		Create a timestamp entry using GPS time.
*/

func setDataLogTimeWithGPS(sit SituationData) {
	/* 
		TODO: we only need to run this function once to set the start time value
		in the current startup record. Calculate the number of milliseconds since the
		system started logging data (stratuxClock init value) and now. Convert the 
		GPS time into milliseconds, then subtract the difference. That gives you the
		start time (UTC) in milliseconds. (In other words, Unix Timestamp in Milliseconds.)
		
		Take that value and stick it in the current "startup" record. 
	
	*/
	if isGPSClockValid() {
		var ts StratuxTimestamp
		// Piggyback a GPS time update from this update.
		ts.id = 0
		ts.Time_type_preference = 1 // gpsClock.
		ts.StratuxClock_value = stratuxClock.Time
		ts.GPSClock_value = sit.GPSTime
		ts.PreferredTime_value = sit.GPSTime

		dataLogTimestamps = append(dataLogTimestamps, ts)
		dataLogCurTimestamp = int64(len(dataLogTimestamps) - 1)
	}
}

/*
	logSituation(), logStatus(), ... pass messages from other functions to the logging
		engine. These are only read into `dataLogChan` if the Replay Log is toggled on,
		and if the log system is ready to accept writes.
*/

func isDataLogReady() bool {
	return dataLogReadyToWrite
}

/*
	findAirport(): a simple, quick process for locating the nearest airport to a given
	set of coordinates. In this case the function is limited to searching within 0.1
	degrees of the input coordinates.
	
	Note: expects to find the "airports.sqlite" file in /root/log
	
	The database is compiled from the FAAs NACAR 56-day subscription database and
	includes all airports including private and heliports.
	
	TODO: Find a source for ALL airports
*/
func findAirport(lat float64, lng float64) (airport, error) {
	
	// return value
	var ret airport

	aptdb, err := sql.Open("sqlite3", "/root/log/airports.sqlite")
	if err != nil {
		return ret, err
	}
	
	defer aptdb.Close()
	
	minLat := lat - 0.1
	minLng := lng - 0.1
	maxLat := lat + 0.1
	maxLng := lng + 0.1
	
	p := geo.NewPoint(lat, lng)
	
	rows, err := aptdb.Query("SELECT faaid, icaoid, name, lat, lng, alt FROM airport WHERE lat > ? AND lat < ? AND lng > ? AND lng < ? ORDER BY id ASC;", minLat, maxLat, minLng, maxLng)
	if err != nil {
		return ret, err
	}
	
	for rows.Next() {
		var r airport
		err = rows.Scan(&r.faaId, &r.icaoId, &r.name, &r.lat, &r.lng, &r.alt)
		ap := geo.NewPoint(r.lat, r.lng)
		r.dst = ap.GreatCircleDistance(p)
		
		if (ret.faaId == "") {
			ret = r
		} else if (r.dst < ret.dst) {
			ret = r
		}
	}
	
	return ret, nil
}

/*
	FlightLog structure - replaces 'startup' structure as the basis for the startup
	table in the SQLite database. A single FlightLog variable is used throughout a
	session (startup) to track flight log information.
*/
type FlightLog struct {
	id int64
	start_airport_id string
	start_airport_name string
	start_timestamp int64
	start_localtime string
	start_tz string
	start_lat float64
	start_lng float64
	
	end_airport_id string
	end_airport_name string
	end_timestamp int64
	end_localtime string
	end_tz string
	end_lat float64
	end_lng float64
	
	duration int64
	distance float64
	groundspeed int64
	
	route string
}

var flightlog FlightLog

/*
	updateFlightLog(): updates the SQLite record for the current startup to indicate
	the appropriate starting and ending values. This is called by dataLogWriter() on
	its thread (it is a go routine) when the dataUpdateChan is flagged.
	
	TODO: replace this with a reflective / introspective automatic update routine ala
	the insert routine used for bulk updates.
*/
func updateFlightLog(db *sql.DB) {
	
	var sql string
	sql = "UPDATE `startup` SET\n"
	sql = sql + "start_airport_id = ?,\n"
	sql = sql + "start_airport_name = ?,\n"
	sql = sql + "start_timestamp = ?,\n"
	sql = sql + "start_localtime = ?,\n"
	sql = sql + "start_tz = ?,\n"
	sql = sql + "start_lat = ?,\n"
	sql = sql + "start_lng = ?,\n"
	sql = sql + "end_airport_id = ?,\n"
	sql = sql + "end_airport_name = ?,\n"
	sql = sql + "end_timestamp = ?,\n"
	sql = sql + "end_localtime = ?,\n"
	sql = sql + "end_tz = ?,\n"
	sql = sql + "end_lat = ?,\n"
	sql = sql + "end_lng = ?,\n"	
	sql = sql + "duration = ?,\n"
	sql = sql + "distance = ?,\n"
	sql = sql + "groundspeed = ?,\n"
	sql = sql + "route = ?\n"
	sql = sql + "WHERE id = ?;"
	
	stmt, err := db.Prepare(sql)
	if err != nil {
		fmt.Printf("Error creating statement: %v", err)
		return
	}
	
	f := flightlog
	ret, err := stmt.Exec(f.start_airport_id, f.start_airport_name, f.start_timestamp, f.start_localtime, f.start_tz, f.start_lat, f.start_lng, f.end_airport_name, f.end_timestamp, f.end_localtime, f.end_tz, f.end_lat, f.end_lng, f.duration, f.distance, f.groundspeed, f.route, stratuxStartupID)
	if err != nil {
		fmt.Printf("Error executing statement: %v\n", err)
		return
	}
	raf, err := ret.RowsAffected()
	if raf < 1 {
		fmt.Println("Error - no rows affected in update")
	}
}

/*
	startFlightLog() - called once per startup when the GPS has a valid timestamp and
	position to tag the beginning of the 'session'. Updates the startup record with
	the initial place / time values.
*/
func startFlightLog() {

	// gps coordinates at startup
	flightlog.start_lat = float64(mySituation.Lat)
	flightlog.start_lng = float64(mySituation.Lng)
	
	// time, timezone, localtime
	flightlog.start_timestamp = mySituation.GPSTime.Unix()
	flightlog.start_tz = latlong.LookupZoneName(float64(mySituation.Lat), float64(mySituation.Lng))
	loc, err := time.LoadLocation(flightlog.start_tz)
	if (err == nil) {
		flightlog.start_localtime = mySituation.GPSTime.In(loc).String()
	}
	
	// airport code and name
	apt, err := findAirport(float64(mySituation.Lat), float64(mySituation.Lng))
	if (err == nil) {
		flightlog.start_airport_id = apt.faaId
		flightlog.start_airport_name = apt.name
	}
	
	// update the database entry
	dataUpdateChan <- true
}

/*
	stopFlightLog() - called every time the system shifts from "flying" state to "taxiing"
	state (or directly to stopped, though that should not happen). Updates the end values
	for the startup record. Appends the stop point airport to the 'route' list, so if
	the aircraft makes multiple stops without powering off the system this will indicate
	all of them.
*/
func stopFlightLog() {

	// gps coordinates at startup
	flightlog.end_lat = float64(mySituation.Lat)
	flightlog.end_lng = float64(mySituation.Lng)
	
	// time, timezone, localtime
	flightlog.end_timestamp = mySituation.GPSTime.Unix()
	flightlog.end_tz = latlong.LookupZoneName(float64(mySituation.Lat), float64(mySituation.Lng))
	loc, err := time.LoadLocation(flightlog.end_tz)
	if (err == nil) {
		flightlog.end_localtime = mySituation.GPSTime.In(loc).String()
	}
	
	// airport code and name
	apt, err := findAirport(float64(mySituation.Lat), float64(mySituation.Lng))
	if (err == nil) {
		flightlog.end_airport_id = apt.faaId
		flightlog.end_airport_name = apt.name
		flightlog.route = flightlog.route + " => " + apt.faaId
	}
	
	// update the database entry
	dataUpdateChan <- true
}

/*
	logSituation() - pushes the current 'mySituation' record into the logging channel
	for writing to the SQLite database. Also provides triggers for startFlightLog(),
	stopFlightLog() and updates the running distance and time tallies for the flight.
*/
func logSituation() {
	if globalSettings.ReplayLog && isDataLogReady() {
		
		// make sure we have valid GPS Clock time
		if (lastTimeStamp == nil) {
			if (isGPSValid() && isGPSClockValid()) {
				startFlightLog()
			} else {
				// not initialized / can't initialize yet - no clock
				return
			}
		}
		
		// update the distance traveled in nautical miles
		p := geo.NewPoint(float64(mySituation.Lat), float64(mySituation.Lng))
		if (lastPoint != nil) {
			segment := p.GreatCircleDistance(lastPoint);
			flightlog.distance = flightlog.distance + (segment * nmPerKm)
		}
		lastPoint = p;
		
		// update the amount of time since startup in milliseconds
		t := stratuxClock.Now()
		if (lastTimestamp != nil) {
			increment := t.Sub(*lastTimestamp)
			flightlog.duration = flightlog.duration + (increment.Nanoseconds() / 1000000)
		}
		lastTimestamp = &t
		
		/*
			Flying status state map:
			
			Standard Transitions
			stop => taxi {normal startup}
			taxi => stop {reposition / back-taxi / run-up}
			taxi => flight {normal takeoff}
			flight => taxi => flight {touch/go landing}
			flight => taxi => stop {full-stop landing}
			
			Abnormal Transitions
			flight => stop {um... not so good}
			stop => flight {mid-air activation}
			
			TODO: add altitude delta check - ignore car trips
			
		*/
		// from dead stop to taxi
		if (weAreFlying == false) && (weAreTaxiing == false) && (mySituation.GroundSpeed < minimumFlightSpeed) && (mySituation.GroundSpeed >= minimumTaxiSpeed) {
			weAreTaxiing = true
		} else
		
		// from dead stop to flying - mid-air start / restart
		if (weAreFlying == false) && (weAreTaxiing == false) && (mySituation.GroundSpeed >= minimumFlightSpeed) {
			weAreFlying = true
			//TODO: how do we handle these?
		} else
		
		// from taxi to flight
		if (weAreTaxiing) && (mySituation.GroundSpeed >= minimumFlightSpeed) {
			weAreTaxiing = false
			weAreFlying = true
		} else
		
		// from flight to taxi
		if (weAreFlying == true) && (mySituation.GroundSpeed < minimumFlightSpeed) && (mySituation.GroundSpeed >= minimumTaxiSpeed) {
			weAreFlying = false
			weAreTaxiing = true
			stopFlightLog()
		} else
		
		// from flight to stop - data glitch? crash?
		if (weAreFlying == true) && (mySituation.GroundSpeed < minimumFlightSpeed) && (mySituation.GroundSpeed < minimumTaxiSpeed) {
			weAreFlying = false
			weAreTaxiing = false
			stopFlightLog()
		} else
		
		// from taxi to stopped
		if (weAreTaxiing) && (mySituation.GroundSpeed < minimumTaxiSpeed) {
			weAreTaxiing = false
		}
		
		// if log level is anything less than DEMO (3), we want to limit the update frequency
		if globalSettings.FlightLogLevel < FLIGHT_LOG_LEVEL_DEMO {
			now := stratuxClock.Milliseconds
			msd := (now - lastSituationLogMs)
			
			// logbook is 30 seconds (30,000 ms)
			if (globalSettings.FlightLogLevel == FLIGHT_LOG_LEVEL_LOGBOOK) && (msd < 30000) {
				return;
			}
			
			// debrief is 2 Hz (500 ms)
			if (globalSettings.FlightLogLevel == FLIGHT_LOG_LEVEL_DEBRIEF) && (msd < 500) {
				return;
			}
			
		}
		
		// only bother to write records if we are moving somehow
		if weAreFlying || weAreTaxiing {
			dataLogChan <- DataLogRow{tbl: "mySituation", data: mySituation}
		}
		lastSituationLogMs = stratuxClock.Milliseconds
	}
}

func logStatus() {
	if globalSettings.ReplayLog && isDataLogReady() {
		dataLogChan <- DataLogRow{tbl: "status", data: globalStatus}
	}
}

func logSettings() {
	if globalSettings.ReplayLog && isDataLogReady() {
		dataLogChan <- DataLogRow{tbl: "settings", data: globalSettings}
	}
}

func logTraffic(ti TrafficInfo) {
	if globalSettings.ReplayLog && isDataLogReady() && (globalSettings.FlightLogLevel > FLIGHT_LOG_LEVEL_DEBRIEF) {
		dataLogChan <- DataLogRow{tbl: "traffic", data: ti}
	}
}

func logMsg(m msg) {
	if globalSettings.ReplayLog && isDataLogReady() && (globalSettings.FlightLogLevel > FLIGHT_LOG_LEVEL_DEBRIEF)  {
		dataLogChan <- DataLogRow{tbl: "messages", data: m}
	}
}

func logESMsg(m esmsg) {
	if globalSettings.ReplayLog && isDataLogReady() && (globalSettings.FlightLogLevel == FLIGHT_LOG_LEVEL_DEBUG) {
		dataLogChan <- DataLogRow{tbl: "es_messages", data: m}
	}
}

func logDump1090TermMessage(m Dump1090TermMessage) {
	if globalSettings.DEBUG && globalSettings.ReplayLog && isDataLogReady() && (globalSettings.FlightLogLevel == FLIGHT_LOG_LEVEL_DEBUG) {
		dataLogChan <- DataLogRow{tbl: "dump1090_terminal", data: m}
	}
}

func initDataLog() {
	//log.Printf("dataLogStarted = %t. dataLogReadyToWrite = %t\n", dataLogStarted, dataLogReadyToWrite) //REMOVE -- DEBUG
	insertString = make(map[string]string)
	insertBatchIfs = make(map[string][][]interface{})
	go dataLogWatchdog()

	//log.Printf("datalog.go: initDataLog() complete.\n") //REMOVE -- DEBUG
}

/*
	dataLogWatchdog(): Watchdog function to control startup / shutdown of data logging subsystem.
		Called by initDataLog as a goroutine. It iterates once per second to determine if
		globalSettings.ReplayLog has toggled. If logging was switched from off to on, it starts
		datalog() as a goroutine. If the log is running and we want it to stop, it calls
		closeDataLog() to turn off the input channels, close the log, and tear down the dataLog
		and dataLogWriter goroutines.
*/

func dataLogWatchdog() {
	for {
		if !dataLogStarted && globalSettings.ReplayLog { // case 1: sqlite logging isn't running, and we want to start it
			log.Printf("datalog.go: Watchdog wants to START logging.\n")
			go dataLog()
		} else if dataLogStarted && !globalSettings.ReplayLog { // case 2:  sqlite logging is running, and we want to shut it down
			log.Printf("datalog.go: Watchdog wants to STOP logging.\n")
			closeDataLog()
		}
		//log.Printf("Watchdog iterated.\n") //REMOVE -- DEBUG
		time.Sleep(1 * time.Second)
		//log.Printf("Watchdog sleep over.\n") //REMOVE -- DEBUG
	}
}

/*
	closeDataLog(): Handler for graceful shutdown of data logging goroutines. It is called by
		by dataLogWatchdog(), gracefulShutdown(), and by any other function (disk space monitor?)
		that needs to be able to shut down sqlite logging without corrupting data or blocking
		execution.

		This function turns off log message reads into the dataLogChan receiver, and sends a
		message to a quit channel ('shutdownDataLogWriter`) in dataLogWriter(). dataLogWriter()
		then sends a message to a quit channel to 'shutdownDataLog` in dataLog() to close *that*
		goroutine. That function sets dataLogStarted=false once the logfile is closed. By waiting
		for that signal, closeDataLog() won't exit until the log is safely written. This prevents
		data loss on shutdown.
*/

func closeDataLog() {
	//log.Printf("closeDataLog(): dataLogStarted = %t\n", dataLogStarted) //REMOVE -- DEBUG
	dataLogReadyToWrite = false // prevent any new messages from being sent down the channels
	log.Printf("datalog.go: Starting data log shutdown\n")
	shutdownDataLogWriter <- true      //
	defer close(shutdownDataLogWriter) // ... and close the channel so subsequent accidental writes don't stall execution
	log.Printf("datalog.go: Waiting for shutdown signal from dataLog()")
	for dataLogStarted {
		//log.Printf("closeDataLog(): dataLogStarted = %t\n", dataLogStarted) //REMOVE -- DEBUG
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("datalog.go: Data log shutdown successful.\n")
}
