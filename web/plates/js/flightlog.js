angular.module('appControllers').controller('FlightlogCtrl', FlightlogCtrl); // get the main module contollers set

StatusCtrl.$inject = ['$rootScope', '$scope', '$state', '$http', '$interval',  '$location', '$window']; // Inject my dependencies

// create our controller function with all necessary logic
function FlightlogCtrl($rootScope, $scope, $state, $location, $window, $http, $interval) {

	$scope.$parent.helppage = 'plates/flightlog-help.html';
	$scope.data_list = [];
	$scope.flight_events = [];
	$scope.ReplayMode = false;
	$scope.currentFlight = 0;
	$scope.speeds = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	$scope.playbackSpeed = 5;
	
	$scope.currentPage = 1;
	$scope.pageSize = 10;
  	$scope.priceSlider = 150;
  	
	$scope.$watch('playbackSpeed', function(newValue){
  		// send the new playback speed to the controller if a playback is active
  		if ($scope.ReplayMode) {
			var replayUrl = "http://" + URL_HOST_BASE + "/replay/speed/" + $scope.playbackSpeed;
			$http.post(replayUrl).
			then(function (response) {
				// do nothing
			}, function (response) {
				// do nothing
			});
  		}
	});
	
	$scope.replayFlight = function (id) {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/play/" + id + "/" + $scope.playbackSpeed + "/0";
		$http.post(replayUrl).
		then(function (response) {
			$scope.CurrentFlight = id;
			getEvents(id);
		}, function (response) {
			// do nothing
		});
	};
	
	$scope.pauseReplay = function() {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/pause";
		$http.post(replayUrl).
		then(function (response) {
			// do nothing
		}, function (response) {
			// do nothing
		});
	}
	
	$scope.stopReplay = function() {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/stop";
		$http.post(replayUrl).
		then(function (response) {
			// do nothing
		}, function (response) {
			// do nothing
		});
	}
	
	$scope.jumpToTimestamp = function(ts, flight) {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/play/" + flight + "/" + $scope.playbackSpeed + "/" + ts;
		$http.post(replayUrl).
		then(function (response) {
			// do nothing
			console.log("Jumped to " + ts);
		}, function (response) {
			// do nothing
		});
	}
	
	$scope.getDetails = function(id) {
		$scope.CurrentFlight = id;
		getEvents(id);
	}
	
	function utcTimeString(epoc) {
		var time = "";
		var val;
		var d = new Date(epoc);
		val = d.getUTCHours();
		time += (val < 10 ? "0" + val : "" + val);
		val = d.getUTCMinutes();
		time += ":" + (val < 10 ? "0" + val : "" + val);
		val = d.getUTCSeconds();
		time += ":" + (val < 10 ? "0" + val : "" + val);
		time += "Z";
		return time;
	}
	
	function secondsToHms(d) {
		d = Number(d);
		var h = Math.floor(d / 3600);
		var m = Math.floor(d % 3600 / 60);
		var s = Math.floor(d % 3600 % 60);
		return ((h > 0 ? h + ":" + (m < 10 ? "0" : "") : "") + m + ":" + (s < 10 ? "0" : "") + s); 
	}

	function getShortDate(m) {
		var sd = ("0" + (m.getUTCMonth()+1)).slice(-2) +"-"+
			("0" + m.getUTCDate()).slice(-2) +"-"+
			m.getUTCFullYear()
		return sd;
	}
	
	function getShortTime(m) {
		var st =
			("0" + m.getUTCHours()).slice(-2) + ":" +
			("0" + m.getUTCMinutes()).slice(-2) + ":" +
			("0" + m.getUTCSeconds()).slice(-2);
		return st;
	}
	
	function getShortTimeLocal(m) {
		var st =
			("0" + m.getHours()).slice(-2) + ":" +
			("0" + m.getMinutes()).slice(-2) + ":" +
			("0" + m.getSeconds()).slice(-2);
		return st;
	}
	
	function connect($scope) {
		if (($scope === undefined) || ($scope === null))
			return; // we are getting called once after clicking away from the status page

		if (($scope.socket === undefined) || ($scope.socket === null)) {
			socket = new WebSocket(URL_STATUS_WS);
			$scope.socket = socket; // store socket in scope for enter/exit usage
		}

		$scope.ConnectState = "Disconnected";

		socket.onopen = function (msg) {
			// $scope.ConnectStyle = "label-success";
			$scope.ConnectState = "Connected";
		};

		socket.onclose = function (msg) {
			// $scope.ConnectStyle = "label-danger";
			$scope.ConnectState = "Disconnected";
			$scope.$apply();
			delete $scope.socket;
			setTimeout(function() {connect($scope);}, 1000);
		};

		socket.onerror = function (msg) {
			// $scope.ConnectStyle = "label-danger";
			$scope.ConnectState = "Error";
			$scope.$apply();
		};

		socket.onmessage = function (msg) {
			//console.log('Received status update.')

			var status = JSON.parse(msg.data)
			// Update Status
			$scope.Version = status.Version;
			$scope.Build = status.Build.substr(0, 10);
			$scope.ReplayMode = status.ReplayMode;

			$scope.$apply(); // trigger any needed refreshing of data
		};
		
		getFlights();
	}

	function setHardwareVisibility() {
		$scope.visible_uat = true;
		$scope.visible_es = true;
		$scope.visible_gps = true;
		$scope.visible_ahrs = true;

		// Simple GET request example (note: responce is asynchronous)
		$http.get(URL_SETTINGS_GET).
		then(function (response) {
			settings = angular.fromJson(response.data);
			$scope.visible_uat = settings.UAT_Enabled;
			$scope.visible_es = settings.ES_Enabled;
			$scope.visible_ping = settings.Ping_Enabled;
			if (settings.Ping_Enabled) {
				$scope.visible_uat = true;
				$scope.visible_es = true;
			}
			$scope.visible_gps = settings.GPS_Enabled;
			$scope.visible_ahrs = settings.AHRS_Enabled;
		}, function (response) {
			// nop
		});
	};

	function getFlights() {
		// Simple GET request example (note: responce is asynchronous)
		$http.get("/flightlog/flights").
		then(function (response) {
			var data = angular.fromJson(response.data);
			var flights = data.data;
			var cnt = flights.length;
			
			for (var i = 0; i < cnt; i++) {
				flight = flights[i];
				var m = new Date(parseInt(flight.start_timestamp));				  
				flight.date = getShortDate(m);
				flight.time = getShortTime(m);
				flight.distance = Math.round(parseFloat(flight.distance), 2);
				flight.hms = secondsToHms(flight.duration);
				console.dir(flight);
			}
			$scope.data_list = flights;
			//$scope.UAT_Towers = cnt;
			//$scope.$apply();
		}, function (response) {
			$scope.raw_data = "error getting tower data";
		});
	};


	function getEvents(id) {
		// Simple GET request example (note: responce is asynchronous)
		$http.get("/flightlog/events/" + id).
		then(function (response) {
			var data = angular.fromJson(response.data);
			var events = data.data;
			var tmp = [];
			var last = "";
			
			var minTS = -1;
			var maxTS = 0;
			
			for (var i = 0; i < events.length; i++) {
				var ce = events[i];
				
				if (ce.event == last) {
					continue;
				}
				
				var evt = {};
				evt.event = ce.event;
				evt.location = ce.airport_name + " (" + ce.airport_id + ")"
				var m = new Date(parseInt(ce.timestamp) * 1000);
				var s = getShortTime(m)
				evt.timeZulu = s
				evt.timeLocal = getShortTimeLocal(m)
				evt.timestamp = parseInt(ce.timestamp_id)
				evt.id = parseInt(ce.id)
				evt.flight = id;
				
				//TODO: do something with this data - create slider / ticker
				if (minTS < 0) {
					minTS = ce.timestamp_id;
				}
				if (ce.timestamp_id < minTS) {
					minTS = ce.timestamp_id;
				}
				if (ce.timestamp_id > maxTS) {
					maxTS = ce.timestamp_id;
				}
				
				last = ce.event;
				
				tmp.push(evt)
			}
			$scope.flight_events = tmp;
			//$scope.UAT_Towers = cnt;
			
			//$scope.$apply();
		}, function (response) {
			$scope.raw_data = "error getting tower data";
		});
	};


	// periodically get the tower list
	var updateTowers = $interval(function () {
		// refresh tower count once each 5 seconds (aka polling)
		//getTowers();
	}, (5 * 1000), 0, false);


	$state.get('home').onEnter = function () {
		// everything gets handled correctly by the controller
	};
	$state.get('home').onExit = function () {
		if (($scope.socket !== undefined) && ($scope.socket !== null)) {
			$scope.socket.close();
			$scope.socket = null;
		}
		$interval.cancel(updateTowers);
	};

	// Status Controller tasks
	setHardwareVisibility();
	connect($scope); // connect - opens a socket and listens for messages
};