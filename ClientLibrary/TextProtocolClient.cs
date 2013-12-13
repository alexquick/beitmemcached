using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using BeIT.MemCached;

namespace BeITMemcached.ClientLibrary
{
	internal class TextProtocolClient : MemcachedClient
	{
		public TextProtocolClient(string name, string[] hosts) : base(name, hosts)
		{
			//no-op
		}

		public override ProtocolType Protocol { get { return ProtocolType.Text; } }

		#region Set, Add, and Replace.

		protected override bool store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry)
		{
			return store(command, key, keyIsChecked, value, hash, expiry, 0).StartsWith("STORED");
		}

		//Hook for the Append and Prepend commands.
		protected override bool store(string command, string key, bool keyIsChecked, object value, uint hash)
		{
			return store(command, key, keyIsChecked, value, hash, 0, 0).StartsWith("STORED");
		}

		//Private overload for the Cas command.
		protected override CasResult store(string key, bool keyIsChecked, object value, uint hash, int expiry, ulong unique)
		{
			string result = store("cas", key, keyIsChecked, value, hash, expiry, unique);
			if (result.StartsWith("STORED")) {
				return CasResult.Stored;
			}
			else if (result.StartsWith("EXISTS")) {
				return CasResult.Exists;
			}
			else if (result.StartsWith("NOT_FOUND")) {
				return CasResult.NotFound;
			}
			return CasResult.NotStored;
		}

		//Private common store method.
		private string store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry, ulong unique)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}

			return serverPool.Execute<string>(hash, "", delegate(PooledSocket socket) {
				SerializedType type;
				byte[] bytes;

				//Serialize object efficiently, store the datatype marker in the flags property.
				try {
					bytes = Serializer.Serialize(value, out type, CompressionThreshold);
				}
				catch (Exception e) {
					//If serialization fails, return false;

					logger.Error("Error serializing object for key '" + key + "'.", e);
					return "";
				}

				//Create commandline
				string commandline = "";
				switch (command) {
					case "set":
					case "add":
					case "replace":
						commandline = command + " " + keyPrefix + key + " " + (ushort) type + " " + expiry + " " + bytes.Length + "\r\n";
						break;
					case "append":
					case "prepend":
						commandline = command + " " + keyPrefix + key + " 0 0 " + bytes.Length + "\r\n";
						break;
					case "cas":
						commandline = command + " " + keyPrefix + key + " " + (ushort) type + " " + expiry + " " + bytes.Length + " " +
									unique + "\r\n";
						break;
				}

				//Write commandline and serialized object.
				socket.Write(commandline);
				socket.Write(bytes);
				socket.Write("\r\n");
				return socket.ReadResponse();
			});
		}

		#endregion

		#region Get

		/// <summary>
		/// This method corresponds to the "get" command in the memcached protocol.
		/// It will return the value for the given key. It will return null if the key did not exist,
		/// or if it was unable to retrieve the value.
		/// If given an array of keys, it will return a same-sized array of objects with the corresponding
		/// values.
		/// Use the overload to specify a custom hash to override server selection.
		/// </summary>
		protected override object get(string command, string key, bool keyIsChecked, uint hash, out ulong unique)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}

			ulong __unique = 0;
			object value = serverPool.Execute<object>(hash, null, delegate(PooledSocket socket) {
				socket.Write(command + " " + keyPrefix + key + "\r\n");
				object _value;
				ulong _unique;
				if (readValue(socket, out _value, out key, out _unique)) {
					socket.ReadLine(); //Read the trailing END.
				}
				__unique = _unique;
				return _value;
			});
			unique = __unique;
			return value;
		}

		/// <summary>
		/// This method executes a multi-get. It will group the keys by server and execute a single get 
		/// for each server, and combine the results. The returned object[] will have the same size as
		/// the given key array, and contain either null or a value at each position according to
		/// the key on that position.
		/// </summary>
		protected override object[] get(string command, string[] keys, bool keysAreChecked, uint[] hashes, out ulong[] uniques)
		{
			//Check arguments.
			if (keys == null || hashes == null) {
				throw new ArgumentException("Keys and hashes arrays must not be null.");
			}
			if (keys.Length != hashes.Length) {
				throw new ArgumentException("Keys and hashes arrays must be of the same length.");
			}
			uniques = new ulong[keys.Length];

			//Avoid going through the server grouping if there's only one key.
			if (keys.Length == 1) {
				return new object[] {get(command, keys[0], keysAreChecked, hashes[0], out uniques[0])};
			}

			//Check keys.
			if (!keysAreChecked) {
				for (int i = 0; i < keys.Length; i++) {
					checkKey(keys[i]);
				}
			}

			//Group the keys/hashes by server(pool)
			Dictionary<SocketPool, Dictionary<string, List<int>>> dict =
				new Dictionary<SocketPool, Dictionary<string, List<int>>>();
			for (int i = 0; i < keys.Length; i++) {
				Dictionary<string, List<int>> getsForServer;
				SocketPool pool = serverPool.GetSocketPool(hashes[i]);
				if (!dict.TryGetValue(pool, out getsForServer)) {
					dict[pool] = getsForServer = new Dictionary<string, List<int>>();
				}

				List<int> positions;
				if (!getsForServer.TryGetValue(keys[i], out positions)) {
					getsForServer[keyPrefix + keys[i]] = positions = new List<int>();
				}
				positions.Add(i);
			}

			//Get the values
			object[] returnValues = new object[keys.Length];
			ulong[] _uniques = new ulong[keys.Length];
			foreach (KeyValuePair<SocketPool, Dictionary<string, List<int>>> kv in dict) {
				serverPool.Execute(kv.Key, delegate(PooledSocket socket) {
					//Build the get request
					StringBuilder getRequest = new StringBuilder(command);
					foreach (KeyValuePair<string, List<int>> key in kv.Value) {
						getRequest.Append(" ");
						getRequest.Append(key.Key);
					}
					getRequest.Append("\r\n");

					//Send get request
					socket.Write(getRequest.ToString());

					//Read values, one by one
					object gottenObject;
					string gottenKey;
					ulong unique;
					while (readValue(socket, out gottenObject, out gottenKey, out unique)) {
						foreach (int position in kv.Value[gottenKey]) {
							returnValues[position] = gottenObject;
							_uniques[position] = unique;
						}
					}
				});
			}
			uniques = _uniques;
			return returnValues;
		}

		//Private method for reading results of the "get" command.
		private bool readValue(PooledSocket socket, out object value, out string key, out ulong unique)
		{
			string response = socket.ReadResponse();
			string[] parts = response.Split(' '); //Result line from server: "VALUE <key> <flags> <bytes> <cas unique>"
			if (parts[0] == "VALUE") {
				key = parts[1];
				byte[] bytes = new byte[Convert.ToUInt32(parts[3], CultureInfo.InvariantCulture)];
				if (parts.Length > 4) {
					unique = Convert.ToUInt64(parts[4]);
				}
				else {
					unique = 0;
				}
				socket.Read(bytes);
				socket.SkipUntilEndOfLine(); //Skip the trailing \r\n
				var type = (SerializedType)Enum.Parse(typeof (SerializedType), parts[2]);
				value = Deserialize(key, type, bytes);
				return true;
			}
			else {
				key = null;
				value = null;
				unique = 0;
				return false;
			}
		}

		#endregion

		#region Delete

		/// <summary>
		/// This method corresponds to the "delete" command in the memcache protocol.
		/// It will immediately delete the given key and corresponding value.
		/// Use the overloads to specify an amount of time the item should be in the delete queue on the server,
		/// or to specify a custom hash to override server selection.
		/// </summary>
		protected override bool delete(string key, bool keyIsChecked, uint hash, int time)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}

			return serverPool.Execute<bool>(hash, false, delegate(PooledSocket socket) {
				string commandline;
				if (time == 0) {
					commandline = "delete " + keyPrefix + key + "\r\n";
				}
				else {
					commandline = "delete " + keyPrefix + key + " " + time + "\r\n";
				}
				socket.Write(commandline);
				return socket.ReadResponse().StartsWith("DELETED");
			});
		}

		#endregion

		#region Increment Decrement

		/// <summary>
		/// This method sets the key to the given value, and stores it in a format such that the methods
		/// Increment and Decrement can be used successfully on it, i.e. decimal representation of a 64-bit unsigned integer. 
		/// Using the overloads it is possible to specify an expiry time, either relative as a TimeSpan or 
		/// absolute as a DateTime. It is also possible to specify a custom hash to override server selection.
		/// This method returns true if the counter was successfully set.
		/// </summary>
		protected override ulong? getCounter(string key, bool keyIsChecked, uint hash)
		{
			ulong parsedLong, unique;
			return ulong.TryParse(get("get", key, keyIsChecked, hash, out unique) as string, out parsedLong)
				? (ulong?) parsedLong
				: null;
		}

		protected override ulong?[] getCounter(string[] keys, bool keysAreChecked, uint[] hashes)
		{
			ulong?[] results = new ulong?[keys.Length];
			ulong[] uniques;
			object[] values = get("get", keys, keysAreChecked, hashes, out uniques);
			for (int i = 0; i < values.Length; i++) {
				ulong parsedLong;
				results[i] = ulong.TryParse(values[i] as string, out parsedLong) ? (ulong?) parsedLong : null;
			}
			return results;
		}

		protected override ulong? incrementDecrement(string cmd, string key, bool keyIsChecked, ulong value, uint hash)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}
			return serverPool.Execute<ulong?>(hash, null, delegate(PooledSocket socket) {
				string command = cmd + " " + keyPrefix + key + " " + value + "\r\n";
				socket.Write(command);
				string response = socket.ReadResponse();
				if (response.StartsWith("NOT_FOUND")) {
					return null;
				}
				else {
					return Convert.ToUInt64(response.TrimEnd('\0', '\r', '\n'));
				}
			});
		}

		#endregion

		#region Flush All

		public override bool FlushAll(TimeSpan delay, bool staggered)
		{
			bool noerrors = true;
			uint count = 0;
			foreach (SocketPool pool in serverPool.HostList) {
				serverPool.Execute(pool, delegate(PooledSocket socket) {
					uint delaySeconds = (staggered ? (uint) delay.TotalSeconds*count : (uint) delay.TotalSeconds);
					//Funnily enough, "flush_all 0" has no effect, you have to send "flush_all" to flush immediately.
					socket.Write("flush_all " + (delaySeconds == 0 ? "" : delaySeconds.ToString()) + "\r\n");
					if (!socket.ReadResponse().StartsWith("OK")) {
						noerrors = false;
					}
					count++;
				});
			}
			return noerrors;
		}

		#endregion

		#region Stats

	

		/// <summary>
		/// This method corresponds to the "stats" command in the memcached protocol.
		/// It will send the stats command to the server that corresponds to the given key, hash or host,
		/// and return a Dictionary containing the results of the command.
		/// </summary>

		protected  override Dictionary<string, string> stats(SocketPool pool)
		{
			if (pool == null) {
				return null;
			}
			Dictionary<string, string> result = new Dictionary<string, string>();
			serverPool.Execute(pool, delegate(PooledSocket socket) {
				socket.Write("stats\r\n");
				string line;
				while (!(line = socket.ReadResponse().TrimEnd('\0', '\r', '\n')).StartsWith("END")) {
					string[] s = line.Split(' ');
					result.Add(s[1], s[2]);
				}
			});
			return result;
		}

		#endregion
	}
}