#! python

import asyncio
from aiohttp import web
from aiohttp_sse import sse_response
import gzip
import sseclient
import requests
from datetime import datetime
import sys
import json
import argparse

# Gotta look pretty on the command line
class TColors:
	END = '\33[0m'

	BLACK   = '\33[30m'
	RED     = '\33[31m'
	GREEN   = '\33[32m'
	YELLOW  = '\33[33m'
	BLUE    = '\33[34m'
	VIOLET  = '\33[35m'
	BEIGE   = '\33[36m'
	WHITE   = '\33[37m'
	GREY    = '\33[90m'
	RED2    = '\33[91m'
	GREEN2  = '\33[92m'
	YELLOW2 = '\33[93m'
	BLUE2   = '\33[94m'
	VIOLET2 = '\33[95m'
	BEIGE2  = '\33[96m'
	WHITE2  = '\33[97m'

# This is the game recording logic
class BlaseballRecorder:
	def __init__(self, filepath=None, uri=None):
		self.filepath = filepath or "blaseballGame"
		self.uri = uri or "https://www.blaseball.com/events/streamData"

		# Options for splitting recordings into day-specific files
		self.day_mode = False
		self.skip_days = 0

		# Internal state
		self.messages = []
		self.last_message = {}
		self.previous_day = -1
		self.previous_season = -1
		self.start_time = datetime.now()

		# Reconnection state
		self.connect_attempts = 0

	# This is the main function for processing new events
	async def listen(self, sse):
		# Grab any new events that the eventStream has given us
		for message in sse.events():
			# If we've gotten an event we've successfully connected, so clear attempts
			self.connect_attempts = 0

			# Time upkeep
			now = datetime.now()
			time_since_start = now - self.start_time

			# Grab the updated data
			data = json.loads(message.data)
			cut_data = data["value"]["games"].copy()

			# Only care about this message if it's new data
			if cut_data != self.last_message:
				# Grab season/day info
				current_season = cut_data["sim"]["season"]
				current_day = cut_data["sim"]["day"]

				# If we're in day mode, check for a new day before adding the new message to the list
				if self.day_mode:
					if self.previous_day < 0:
						self.previous_day = current_day
						self.previous_season = current_season
					elif current_day > self.previous_day or current_season > self.previous_season:
						# New day found! So write off what we've got and clear the recording data
						print(f"Day change: s{self.previous_season}d{self.previous_day} -> s{current_season}d{current_day}")
						self.write(self.messages)
						self.previous_day = current_day
						self.previous_season = current_season
						self.messages = []
						self.start_time = now

						# New file means new 0.0 time
						time_since_start = now - now

				# Set us up to check against the new cut_data, and add the full data to list
				self.last_message = cut_data
				self.messages.append((time_since_start.total_seconds(), data))

				# Purely for display on the command line
				total_games = 0
				ongoing_games = 0
				finished_games = 0
				for game in cut_data["schedule"]:
					total_games += 1
					if game["gameComplete"]:
						finished_games += 1
					elif game["gameStart"]:
						ongoing_games += 1
				print(f"{TColors.BLUE2}{time_since_start.total_seconds():7.2f}{TColors.END}: s{current_season}d{current_day} Ongoing:{ongoing_games}/{total_games} Finished:{finished_games}/{total_games}")

	# This is the main function for handling the connection
	async def record(self):
		# Make sure to note the time we start, so time-accurate playback is captured
		self.start_time = datetime.now()

		# Reconnect loop, 5 attempts chosen arbitrarily
		while self.connect_attempts < 5:
			try:
				# requests.get opens the connection
				response = requests.get(self.uri, stream=True)
				# Create a client object from the connection
				sse = sseclient.SSEClient(response)

				# Await on this, which will process new messages indefiniately so long as the connection remains open
				await asyncio.gather(self.listen(sse))
				
				# If we've hit this code, the connection has closed itself from the server without error
				self.connect_attempts += 1
				print(f"{TColors.RED2}Reconnecting {self.connect_attempts}/5...{TColors.END}")
			except requests.exceptions.RequestException as e:
				# If we're here, there's been an unclean issue with the connection
				# Most of the time it's a chunk error, which I think means we've lost a packet of the pushed event?
				print(f"{TColors.RED2}REQUEST EXCEPTION:{TColors.END} {e.__class__}, reconnecting after a delay")

				# Do a longer delay each attempt to give things time to sort out
				await asyncio.sleep(5 * self.connect_attempts)
				self.connect_attempts += 1
				print(f"{TColors.RED2}Reconnecting {self.connect_attempts}/5...{TColors.END}")

		# If we've failed to reconnect, write and close
		self.write(self.messages)
		sys.exit(0)

	# As the name suggets, this is the main function for writing our files out
	def write(self, messages, error=False):
		path = self.filepath

		# If we're in day mode, add the season/day to the filepath
		if self.day_mode:
			# Don't write the file if we didn't have a valid day or if we're skipping it
			if self.previous_day < 0:
				return
			if self.skip_days > 0:
				self.skip_days -= 1
				print(f"{TColors.RED}Skipping day, {self.skip_days} skips remaining{TColors.END}")
				return
			path += f".s{self.previous_season}d{self.previous_day}"

		# If we're in erorr mode, write an error dump so it won't clash with an actual file for the day
		if error:
			print(f"{TColors.RED}DUMPING DUE TO ERROR{TColors.END}")
			path += ".errorDump"

		# Original file format do not steal
		path += ".stream"

		# Do the thing
		if len(messages) < 1:
			print(f"{TColors.RED}Skipping file write, no messages{TColors.END}")
			return
		file = open(path, 'w')
		json.dump(messages, file)
		print(f"{TColors.RED}Recorded {TColors.YELLOW2}{len(messages)}{TColors.RED} messages to {TColors.GREEN2}{path}{TColors.END}")

	# This is the entry point for actually recording stuff
	def start(self):
		try:
			asyncio.get_event_loop().run_until_complete(self.record())
		except KeyboardInterrupt:
			# Make sure keyboard interrupts close us cleanly
			self.write(self.messages)
			sys.exit(0)
			return
		except:
			# All other unhandled issues are errors :(
			self.write(self.messages, True)
			raise

# This is the playback logic
class BlaseballStreamer:
	def __init__(self, filepath=None, archive=False):
		# Load up the given filepath
		if not archive:
			filepath = filepath or "blaseballGame.stream"
			self.messages = FileQueue(filepath)
			print(f"{TColors.RED}Loaded {TColors.YELLOW2}{len(self.messages)}{TColors.RED} messages from {TColors.GREEN2}{filepath}{TColors.RED}, last at {TColors.BLUE2}{self.messages.bottom()[0]:7.2f}{TColors.END}")
		else:
			# Load a compressed file from SIBR's s3 archives
			self.messages = ArchiveQueue(filepath)
		
		# Default settings for playback
		self.speed = 1.0
		self.http = False
		self.sse = True

		# Internals
		self.webapp = web.Application()
		self.http_games = []
		self._sse_lock = False  # only one event stream can be open at a time due to mutable state in the playback queue.

	# This holds the update logic for HTTP playback
	# This will play back a stream in constant time, updating a cache of game data as it goes
	# That will then be accessable from localhost:8080/games, as if using the blaseball /games api
	async def start_http_playback(self):
		# Setup variables
		start_time = datetime.now()
		last_message_body = {}

		print(f"{TColors.RED}Started HTTP playback{TColors.END}")

		# While we still have messages left to show...
		while not self.messages.is_empty():
			# Are we past the timestamp of the next message?
			now = datetime.now()
			time_since_start = now - start_time
			seconds = time_since_start.total_seconds() * self.speed

			if seconds >= self.messages.top()[0]:
				# We are? Then update the current cache of what to show if somebody accesses the endpoint
				print(f"{TColors.GREEN}HTTP: {TColors.BLUE}{self.messages.top()[0]:7.2f}{TColors.END}/{self.messages.bottom()[0]:7.2f}\r")
				message = self.messages.top()[1]

				if isinstance(message, int):
					# If it's just an int, that means we have identical content to the previous message
					# Was used when the stream included lastUpdateTime to indicate we got a message with a new lastUpdateTime that was identical
					message = last_message_body
				else:
					last_message_body = message

				# Grab the schedule, since that's the json we care about
				self.http_games = message["value"]["games"]["schedule"]

				# Advance
				self.messages.pop()
			else:
				# If we're not to the next message's timestamp yet, sleep for a bit
				await asyncio.sleep(0.05)

		# We've hit the end of the stream
		print(f"{TColors.RED}Finished HTTP playback{TColors.END}")

	# This method starts the server
	async def start_server(self, webapp):
		runner = web.AppRunner(webapp)
		await runner.setup()
		site = web.TCPSite(runner, 'localhost', 8080)
		await site.start()
		while True:
			# Sleep this forever to keep things open
			await asyncio.sleep(100)

	# This method handles requests for /games
	async def handle_http_games(self, request):
		return web.json_response(self.http_games)

	# This method handles requests for /streamData as an eventSource
	async def handle_sse(self, request):
		if self._sse_lock:
			raise Exception('Only one open stream is currently supported.')
		try:
			self._sse_lock = True
			# This sets up the eventSource stream
			async with sse_response(request) as resp:
				# Setup variables
				start_time = datetime.now()
				last_message_body = {}

				print(f"{TColors.RED}Started playback due to SSE connection{TColors.END}")

				# While we still have messages left to show...
				while not self.messages.is_empty():
					# Are we past the timestamp of the next message?
					now = datetime.now()
					time_since_start = now - start_time
					seconds = time_since_start.total_seconds() * self.speed

					if seconds >= self.messages.top()[0]:
						# We are? Then send a new event
						print(f"{TColors.GREEN}SSE: {TColors.BLUE2}{self.messages.top()[0]:7.2f}/{self.messages.bottom()[0]:7.2f}{TColors.END}\r")
						message = self.messages.top()[1]

						if isinstance(message, int):
							# If it's just an int, that means we have identical content to the previous message
							# Was used when the stream included lastUpdateTime to indicate we got a message with a new lastUpdateTime that was identical
							message = last_message_body
							message["value"]["lastUpdateTime"] = self.messages.top()[1]
						else:
							last_message_body = message

						# Actually send the event
						await resp.send(json.dumps(message))

						# Advance
						self.messages.pop()
					else:
						# If we're not to the next message's timestamp yet, sleep for a bit
						await asyncio.sleep(0.05)

				# We've hit the end of the stream
				print(f"{TColors.RED}Finished SSE playback{TColors.END}")

			# Close connection
			return resp
		finally:
			self._sse_lock = False

	# This method starts us up in http mode
	async def start_http(self):
		print(f"{TColors.BEIGE2}Starting stream in HTTP mode...{TColors.END}")
		await asyncio.gather(
			self.start_http_playback(),
			self.start_server(self.webapp)
		)

	# This method starts us up in sse (aka eventSource) mode
	async def start_sse(self):
		print(f"{TColors.BEIGE2}Starting stream in SSE mode...{TColors.END}")
		await asyncio.gather(
			self.start_server(self.webapp)
		)

	# Entry point
	def start(self):
		try:
			if self.http:
				self.webapp.add_routes([web.get('/games', self.handle_http_games)])
				asyncio.get_event_loop().run_until_complete(self.start_http())
			elif self.sse:
				self.webapp.add_routes([web.get('/streamData', self.handle_sse)])
				asyncio.get_event_loop().run_until_complete(self.start_sse())
			else:
				print(f"The birds prevent any streaming from happening. Check your options.")
		except KeyboardInterrupt:
			print(f"{TColors.BEIGE2}Received interrupt, shutting down stream{TColors.END}")
			sys.exit(0)


class FileQueue:
	"""
	Simple deque abstraction to easily peek top and advance messages.
	"""
	def __init__(self, data):
		with open(data or "blaseballGame.stream", "r") as f:
			self._data = json.load(f)
		self._original_len = len(self._data)

	def is_empty(self):
		"""
		Returns True if we have reached the end of file.
		"""
		return not self._data

	def top(self):
		"""
		Returns the next (timedelta, payload) tuple without advancing the queue.
		"""
		if self.is_empty():
			return None
		return self._data[0]

	def bottom(self):
		"""
		Returns the last (timedelta, payload) tuple.
		"""
		if self.is_empty():
			return None
		return self._data[-1]

	def pop(self):
		"""
		Returns the next (timedelta, payload) and advances the queue.
		"""
		if self.is_empty():
			return None
		return self._data.pop(0)

	def __len__(self):
		"""
		Return length of original log file.
		"""
		return self._original_len


class ArchiveQueue(FileQueue):
	"""
	Decompresses a SIBR archive file reads the file one line at a time so as not to blow up RAM,
	faking the timedelta for messages to get emitted every second (approximately in line with
	what we get from prod)
	"""

	def __init__(self, data):
		# data is a file name
		self._data = gzip.open(data, mode='rt', encoding='utf-8')
		self._cur_message = self._next_message()
		if not self._cur_message:
			# idk how we got here
			return
		self._start_ts = self._message_timestamp(self._cur_message)

	def _next_message(self):
		try:
			msg = next(self._data)
			self._cur_message = {'value': {'games': json.loads(msg)}}
		except StopIteration:
			self._cur_message = None
		return self._cur_message

	def _message_timestamp(self, msg):
		return msg['value']['games']['clientMeta']['timestamp'] / 1000.0

	def is_empty(self):
		"""
		Returns true of we have no more messages to load.
		"""
		return self._cur_message is None

	def top(self):
		"""
		Returns next (timedelta, payload) tuple without advancing the queue.
		"""
		if self.is_empty():
			return 0.0, None
		return self._message_timestamp(self._cur_message) - self._start_ts, self._cur_message

	def bottom(self):
		"""
		Dummy implementation so as not to break the log-based code.
		"""
		return 0.0, None

	def pop(self):
		"""
		Returns next (timedelta, payload) tuple and advances the queue.
		"""
		next_message = self._cur_message
		self._cur_message = self._next_message()
		return self._message_timestamp(next_message) - self._start_ts, next_message

	def __len__(self):
		"""
		Dummy implementaiotn so as not to break the log-based code.
		"""
		return 1

	def __del__(self):
		"""
		Close log file on exit.
		"""
		if self._data:
			self._data.close()


# Handles starting us in record mode from the command line arguments
def handle_record(args):
	bbr = BlaseballRecorder(args.filepath, args.uri)
	bbr.day_mode = args.day
	bbr.skip_days = args.skipdays
	bbr.start()

# Handles starting us in stream mode form the command line arguments
def handle_stream(args):
	bbs = BlaseballStreamer(args.filepath, archive=args.archive)
	bbs.speed = args.speed

	# You can only be one of these
	bbs.http = args.http and not args.sse
	bbs.sse = args.sse or not bbs.http

	bbs.start()

# Setup the command line parser
parser = argparse.ArgumentParser(description="Record and Stream Blaseball game data feeds")
subparsers = parser.add_subparsers(title="Modes", dest="mode", required=True, description="Available modes", help="Which mode the program should run in")

# Recording mode subparser
record_parser = subparsers.add_parser("record", help="Records a live websocket stream to a file")
record_parser.set_defaults(func=handle_record)
record_parser.add_argument("-f", "--file", dest="filepath", help="Path to save the recording to")
record_parser.add_argument("--uri", help="URI to connect to using a websocket")
record_parser.add_argument("--day", action="store_true", help="Appends the current season and day to the filename, and writes out the prevoius day to a file when the current day changes")
record_parser.add_argument("--skipdays", type=int, default=0, help="Skip this number of days before starting to write day files. Useful with a value of 1 to not overwrite an existing day.")

# Streaming mode subparser
stream_parser = subparsers.add_parser("stream", help="Streams a given recording back to websocket or HTTP polling")
stream_parser.set_defaults(func=handle_stream)
stream_parser.add_argument("-f", "--file", dest="filepath", help="Path to read the stream data from")
stream_parser.add_argument("--http", action="store_true", help="Use HTTP playback to localhost:8080/game for polling implementations")
stream_parser.add_argument("--sse", action="store_true", help="Use SSE playback to localhost:8080/streamGameData for SSE implementation. If neither this or --http is given, default to this")
stream_parser.add_argument("--speed", type=float, default=1.0, help="Playback rate, as a float")
stream_parser.add_argument("--archive", action="store_true", help="specified playback file is an archive file from SIBR.")

args = parser.parse_args()
if args.func:
	args.func(args)
else:
	print(f"The Umpire incinerated your command :(")
