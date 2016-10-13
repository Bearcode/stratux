angular.module('appControllers').controller('FlightlogCtrl', FlightlogCtrl); // get the main module contollers set

StatusCtrl.$inject = ['$rootScope', '$scope', '$state', '$http', '$interval',  '$location', '$window']; // Inject my dependencies

// create our controller function with all necessary logic
function FlightlogCtrl($rootScope, $scope, $state, $location, $window, $http, $interval) {

	$scope.$parent.helppage = 'plates/flightlog-help.html';
	$scope.data_list = [];
	$scope.flight_events = [];
	$scope.ReplayMode = false;
	$scope.ReplayPaused = false;
	$scope.currentFlight = 0;
	$scope.playbackSpeed = 1;
	$scope.currentPage = 1;
	$scope.pageSize = 10;
	$scope.selectedIndex = -1;
	$scope.flightToDelete = -1;
	
  	$scope.timeSlider = {
  		value: 0,
  		options: {
  			ceil: 100,
  			floor: 0,
  			hideLimitLabels: true,
  			disabled: true,
  			translate: function(value) {
  				return secondsToHms(value);
  			},
  			onChange: function () {
  				var ts = ($scope.timeSlider.value * 1000);
				var replayUrl = "http://" + URL_HOST_BASE + "/replay/play/" + $scope.currentFlight + "/" + $scope.playbackSpeed + "/" + ts;
				$http.post(replayUrl).
				then(function (response) {
					// do nothing
					console.log("Jumped to " + ts);
				}, function (response) {
					// do nothing
				});
            }
  		}
  	};
  	
  	$scope.$watch('ReplayMode', function(newValue){
  		console.log("Replay mode changed: " + newValue);
  		$scope.timeSlider.options.disabled = !newValue;
  	});
  	
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
	
	$scope.replayFlight = function () {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/play/" + $scope.currentFlight + "/" + $scope.playbackSpeed + "/0";
		$http.post(replayUrl).
		then(function (response) {
			$scope.ReplayPaused = false;
			getEvents(id);
		}, function (response) {
			// do nothing
		});
	};
	
	$scope.pauseReplay = function() {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/pause";
		$http.post(replayUrl).
		then(function (response) {
			$scope.ReplayPaused = true;
		}, function (response) {
			// do nothing
		});
	}
	
	$scope.resumeReplay = function() {
		var replayUrl = "http://" + URL_HOST_BASE + "/replay/resume";
		$http.post(replayUrl).
		then(function (response) {
			$scope.ReplayPaused = false;
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
	
	$scope.preDeleteFlight = function (id) {
		$scope.flightToDelete = id;
	};
	
	$scope.deleteFlight = function () {
		var replayUrl = "http://" + URL_HOST_BASE + "/flightlog/delete/" + $scope.flightToDelete;
		$http.post(replayUrl).
		then(function (response) {
			if ($scope.currentFlight == id) {
				$scope.currentFlight = 0;
			}
		}, function (response) {
			// do nothing
		});
	};
	
	$scope.getDetails = function($index, id) {
		$scope.selectedIndex = $index;
		$scope.currentFlight = id;
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
	
	function replay($scope) {
	
		if (($scope === undefined) || ($scope === null))
			return; // we are getting called once after clicking away from the status page

		if (($scope.rsocket === undefined) || ($scope.rsocket === null)) {
			rsocket = new WebSocket(URL_REPLAY_WS);
			$scope.rsocket = rsocket; // store socket in scope for enter/exit usage
		}

		$scope.RConnectState = "Disconnected";

		rsocket.onopen = function (msg) {
			// $scope.ConnectStyle = "label-success";
			$scope.RConnectState = "Connected";
		};

		rsocket.onclose = function (msg) {
			// $scope.ConnectStyle = "label-danger";
			$scope.RConnectState = "Disconnected";
			$scope.$apply();
			delete $scope.rsocket;
			setTimeout(function() {replay($scope);}, 1000);
		};

		rsocket.onerror = function (msg) {
			// $scope.ConnectStyle = "label-danger";
			$scope.RConnectState = "Error";
			$scope.$apply();
		};

		rsocket.onmessage = function (msg) {
			//console.log('Received status update.')

			var status = JSON.parse(msg.data)
			
			// Update Status
			$scope.timeSlider.value = (parseInt(status.Timestamp) / 1000);
			
			var flight = parseInt(status.Flight);
			var speed = parseInt(status.Speed);
			if (($scope.currentFlight != flight) && (flight != 0)) {
				$scope.currentFlight = flight;
				getEvents(flight);
			}
			if (($scope.playbackSpeed != speed) && (flight != 0)) {
				$scope.playbackSpeed = speed;
			}

			$scope.$apply(); // trigger any needed refreshing of data
		};
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

	function getFlights() {
		// Simple GET request example (note: responce is asynchronous)
		$http.get("/flightlog/flights").
		then(function (response) {
			var data = angular.fromJson(response.data);
			var flights = data.data;
			var cnt = flights.length;
			
			for (var i = 0; i < cnt; i++) {
				flight = flights[i];
				flight.id = parseInt(flight.id);
				var m = new Date(parseInt(flight.start_timestamp));				  
				flight.date = getShortDate(m);
				flight.time = getShortTime(m);
				flight.distance = Math.round(parseFloat(flight.distance), 2);
				flight.hms = secondsToHms(flight.duration);
			}
			$scope.data_list = flights;
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
				evt.location = ce.airport_id; //ce.airport_name + " (" + ce.airport_id + ")"
				var m = new Date(parseInt(ce.timestamp) * 1000);
				evt.timeZulu = getShortTime(m)
				evt.timeLocal = getShortTimeLocal(m)
				var seconds = ce.timestamp_id / 1000;
				evt.seconds = seconds;
				evt.timeHMS = secondsToHms(seconds);
				evt.timestamp = parseInt(ce.timestamp_id)
				evt.id = parseInt(ce.id)
				evt.flight = id;
				
				//TODO: do something with this data - create slider / ticker
				if (minTS < 0) {
					minTS = parseInt(ce.timestamp_id);
				}
				if (ce.timestamp_id < minTS) {
					minTS = parseInt(ce.timestamp_id);
				}
				if (ce.timestamp_id > maxTS) {
					maxTS = parseInt(ce.timestamp_id);
				}
				
				last = ce.event;
				
				tmp.push(evt)
			}
			
			var diff = (maxTS - minTS);
			
			$scope.timeSlider.options.floor = minTS / 1000;
			$scope.timeSlider.options.ceil = maxTS / 1000;
			$scope.flight_events = tmp;
			//$scope.UAT_Towers = cnt;
			
			//$scope.$apply();
		}, function (response) {
			$scope.raw_data = "error getting tower data";
		});
	};


	$state.get('home').onEnter = function () {
		// everything gets handled correctly by the controller
	};
	
	$state.get('home').onExit = function () {
		if (($scope.socket !== undefined) && ($scope.socket !== null)) {
			$scope.socket.close();
			$scope.socket = null;
		}
		if (($scope.rsocket !== undefined) && ($scope.rsocket !== null)) {
			$scope.rsocket.close();
			$scope.rsocket = null;
		}
	};

	// Flightlog Controller tasks
	connect($scope); // connect - opens a socket and listens for messages
	replay($scope);
};