#! python

import asyncio
from aiohttp import web
from aiohttp_sse import sse_response
import sseclient
import requests
from datetime import datetime
import sys
import json
import argparse

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

class BlaseballRecorder:
	def __init__(self, filepath=None, uri=None):
		self.filepath = filepath or "blaseballGame"
		self.uri = uri or "https://www.blaseball.com/events/streamGameData"
		self.day_mode = False
		self.skip_days = 0
		self.messages = []
		self.last_message = {}
		self.previous_day = -1
		self.previous_season = -1
		self.start_time = datetime.now()
		self.last_update_time = -1
		self.connect_attempts = 0

	async def listen(self, sse):
		skips = 0
		for message in sse.events():
			self.connect_attempts = 0
			now = datetime.now()
			time_since_start = now - self.start_time
			data = json.loads(message.data)
			cut_data = data["value"].copy()
			lut = cut_data.pop("lastUpdateTime", -1)
			if lut < self.last_update_time:
				skips += 1
				continue
			if skips > 0:
				print(f"Skipped {skips} messages older than our last known update")
				skips = 0
			self.last_update_time = lut
			if cut_data != self.last_message:
				current_season = cut_data["sim"]["season"]
				current_day = cut_data["sim"]["day"]
				if self.day_mode:
					if self.previous_day < 0:
						self.previous_day = current_day
						self.previous_season = current_season
					elif current_day > self.previous_day or current_season > self.previous_season:
						print(f"Day change: s{self.previous_season}d{self.previous_day} -> s{current_season}d{current_day}")
						self.write(self.messages)
						self.previous_day = current_day
						self.previous_season = current_season
						self.messages = []
						self.start_time = now
						time_since_start = now - now
				self.last_message = cut_data
				self.messages.append((time_since_start.total_seconds(), data))
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
			else:
				self.messages.append((time_since_start.total_seconds(), lut))

	async def record(self):
		self.start_time = datetime.now()
		while self.connect_attempts < 5:
			response = requests.get(self.uri, stream=True)
			sse = sseclient.SSEClient(response)
			await asyncio.gather(self.listen(sse))
			self.connect_attempts += 1
			print(f"{TColors.RED2}Reconnecting {self.connect_attempts}/5...{TColors.END}")
			self.last_message = {}
		self.write(self.messages)
		sys.exit(0)

	def write(self, messages, error=False):
		path = self.filepath
		if self.day_mode:
			if self.previous_day < 0:
				return
			if self.skip_days > 0:
				self.skip_days -= 1
				print(f"{TColors.RED}Skipping day, {self.skip_days} skips remaining{TColors.END}")
				return
			path += f".s{self.previous_season}d{self.previous_day}"
		if error:
			print(f"{TColors.RED}DUMPING DUE TO ERROR{TColors.END}")
			path += ".errorDump"
		path += ".stream"
		if len(messages) < 1:
			print(f"{TColors.RED}Skipping file write, no messages{TColors.END}")
			return
		file = open(path, 'w')
		json.dump(messages, file)
		print(f"{TColors.RED}Recorded {TColors.YELLOW2}{len(messages)}{TColors.RED} messages to {TColors.GREEN2}{path}{TColors.END}")

	def start(self):
		try:
			asyncio.get_event_loop().run_until_complete(self.record())
		except KeyboardInterrupt:
			self.write(self.messages)
			sys.exit(0)
			return
		except:
			self.write(self.messages, True)
			raise

class BlaseballStreamer:
	def __init__(self, filepath=None):
		filepath = filepath or "blaseballGame.stream"
		file = open(filepath, 'r')
		self.messages = json.load(file)
		print(f"{TColors.RED}Loaded {TColors.YELLOW2}{len(self.messages)}{TColors.RED} messages from {TColors.GREEN2}{filepath}{TColors.RED}, last at {TColors.BLUE2}{self.messages[-1][0]:7.2f}{TColors.END}")
		
		self.speed = 1.0
		self.http = False
		self.sse = True

		self.webapp = web.Application()
		self.http_games = []

	async def start_http_playback(self):
		start_time = datetime.now()
		next_message = 0
		last_message_body = {}
		print(f"{TColors.RED}Started HTTP playback{TColors.END}")
		while len(self.messages) > next_message:
			now = datetime.now()
			time_since_start = now - start_time
			seconds = time_since_start.total_seconds() * self.speed
			if seconds >= self.messages[next_message][0]:
				print(f"{TColors.GREEN}HTTP: {TColors.BLUE}{self.messages[next_message][0]:7.2f}{TColors.END}/{self.messages[-1][0]:7.2f}\r")
				message = self.messages[next_message][1]
				if isinstance(message, int):
					message = last_message_body
				else:
					last_message_body = message
				self.http_games = message["value"]["schedule"]
				next_message += 1
			else:
				await asyncio.sleep(0.05)
		print(f"{TColors.RED}Finished HTTP playback{TColors.END}")

	async def start_http_server(self, webapp):
		runner = web.AppRunner(webapp)
		await runner.setup()
		site = web.TCPSite(runner, 'localhost', 8080)
		await site.start()
		while True:
			await asyncio.sleep(100)

	async def handle_http_games(self, request):
		return web.json_response(self.http_games)

	async def handle_sse(self, request):
		async with sse_response(request) as resp:
			start_time = datetime.now()
			next_message = 0
			last_message_body = {}
			print(f"{TColors.RED}Started playback due to SSE connection{TColors.END}")

			while len(self.messages) > next_message:
				now = datetime.now()
				time_since_start = now - start_time
				seconds = time_since_start.total_seconds() * self.speed
				if seconds >= self.messages[next_message][0]:
					print(f"{TColors.GREEN}SSE: {TColors.BLUE2}{self.messages[next_message][0]:7.2f}/{self.messages[-1][0]:7.2f}{TColors.END}\r")
					message = self.messages[next_message][1]
					if isinstance(message, int):
						message = last_message_body
						message["value"]["lastUpdateTime"] = self.messages[next_message][1]
					else:
						last_message_body = message
					await resp.send(json.dumps(message))
					next_message += 1
				else:
					await asyncio.sleep(0.05)
			print(f"{TColors.RED}Finished SSE playback{TColors.END}")
		return resp

	async def start_http(self):
		print(f"{TColors.BEIGE2}Starting stream in HTTP mode...{TColors.END}")
		await asyncio.gather(
			self.start_http_playback(),
			self.start_http_server(self.webapp)
		)

	async def start_sse(self):
		print(f"{TColors.BEIGE2}Starting stream in SSE mode...{TColors.END}")
		await asyncio.gather(
			self.start_http_server(self.webapp)
		)

	def start(self):
		try:
			if self.http:
				self.webapp.add_routes([web.get('/games', self.handle_http_games)])
				asyncio.get_event_loop().run_until_complete(self.start_http())
			elif self.sse:
				self.webapp.add_routes([web.get('/streamGameData', self.handle_sse)])
				asyncio.get_event_loop().run_until_complete(self.start_sse())
			else:
				print(f"The birds prevent any streaming from happening. Check your options.")
		except KeyboardInterrupt:
			print(f"{TColors.BEIGE2}Received interrupt, shutting down stream{TColors.END}")
			sys.exit(0)

def handle_record(args):
	bbr = BlaseballRecorder(args.filepath, args.uri)
	bbr.day_mode = args.day
	bbr.skip_days = args.skipdays
	bbr.start()

def handle_stream(args):
	bbs = BlaseballStreamer(args.filepath)
	bbs.speed = args.speed
	bbs.http = args.http and not args.sse
	bbs.sse = args.sse or not bbs.http
	bbs.start()

parser = argparse.ArgumentParser(description="Record and Stream Blaseball game data feeds")
subparsers = parser.add_subparsers(title="Modes", dest="mode", required=True, description="Available modes", help="Which mode the program should run in")

record_parser = subparsers.add_parser("record", help="Records a live websocket stream to a file")
record_parser.set_defaults(func=handle_record)
record_parser.add_argument("-f", "--file", dest="filepath", help="Path to save the recording to")
record_parser.add_argument("--uri", help="URI to connect to using a websocket")
record_parser.add_argument("--day", action="store_true", help="Appends the current season and day to the filename, and writes out the prevoius day to a file when the current day changes")
record_parser.add_argument("--skipdays", type=int, default=0, help="Skip this number of days before starting to write day files. Useful with a value of 1 to not overwrite an existing day.")

stream_parser = subparsers.add_parser("stream", help="Streams a given recording back to websocket or HTTP polling")
stream_parser.set_defaults(func=handle_stream)
stream_parser.add_argument("-f", "--file", dest="filepath", help="Path to read the stream data from")
stream_parser.add_argument("--http", action="store_true", help="Use HTTP playback to localhost:8080/game for polling implementations")
stream_parser.add_argument("--sse", action="store_true", help="Use SSE playback to localhost:8080/streamGameData for SSE implementation. If neither this or --http is given, default to this")
stream_parser.add_argument("--speed", type=float, default=1.0, help="Playback rate, as a float")

args = parser.parse_args()
if args.func:
	args.func(args)
else:
	print(f"The Umpire incinerated your command :(")