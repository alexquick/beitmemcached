using System;
using System.Collections.Generic;
using BeIT.MemCached;

namespace BeITMemcached.ClientLibrary.Binary
{
	internal class BinaryProtocolClient : MemcachedClient
	{
		public BinaryProtocolClient(string name, string[] hosts) : base(name, hosts)
		{
		}

		public override ProtocolType Protocol
		{
			get { return ProtocolType.Binary; }
		}

		protected override bool store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry)
		{
			return store(command, key, keyIsChecked, value, hash, expiry, 0).ResponseCode == ErrorCode.None;
		}

		//Hook for the Append and Prepend commands.
		protected override bool store(string command, string key, bool keyIsChecked, object value, uint hash)
		{
			return store(command, key, keyIsChecked, value, hash, 0, 0).ResponseCode == ErrorCode.None;
		}

		protected override bool store(string command, string[] keys, bool keyIsChecked, object[] values, uint[] hashes,
			int expiry)
		{
			BinaryResponse[] responses = storeBinary(command, keys, keyIsChecked, values, hashes, expiry, null);
			foreach (var response in responses) {
				if (response!=null && response.ResponseCode != ErrorCode.None) {
					return false;
				}
			}
			return true;
		}

		protected override bool store(string command, string[] keys, bool keyIsChecked, object[] values, uint[] hashes)
		{
			EnsureAlignment(keys, values, hashes);
			bool success = true;
			for (int i = 0; i < keys.Length; i++) {
				success &= store(command, keys[i], keyIsChecked, values[i], hashes[i]);
			}
			return success;
		}

		protected override CasResult store(string key, bool keyIsChecked, object value, uint hash, int expiry, ulong unique)
		{
			throw new NotImplementedException();
		}

		private BinaryResponse[] storeBinary(string command, string[] keys, bool keyIsChecked, object[] values, uint[] hashes, int expiry,
			ulong[] unique)
		{
			if (!keyIsChecked) {
				foreach (var key in keys) {
					checkKey(key);
				}
			}

			var opcode = Opcode.Parse(command);
			var requestTemplate = new BinaryRequest() {
				Opcode = opcode,
			};
			return executeBulk(requestTemplate, keys, hashes, values);
		}

		private BinaryResponse store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry,
			ulong unique)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}

			return serverPool.Execute<BinaryResponse>(hash, null, delegate(PooledSocket socket) {
				SerializedType type;
				byte[] bytes;

				//Serialize object efficiently, store the datatype marker in the flags property.
				try {
					bytes = Serializer.Serialize(value, out type, CompressionThreshold);
				} catch (Exception e) {
					//If serialization fails, return false;

					logger.Error("Error serializing object for key '" + key + "'.", e);
					return null;
				}
				byte[] extras = new byte[8];
				Array.Copy(BinaryMessage.NetworkOrder(BitConverter.GetBytes((int)type)), 0, extras, 0, 4);
				Array.Copy(BinaryMessage.NetworkOrder(BitConverter.GetBytes(expiry)), 0, extras, 4, 4);
				var request = new BinaryRequest() {
					Extras = extras,
					Value = bytes,
					KeyAsString = keyPrefix + key
				};
				request.Opcode = Opcode.Parse(command);
				if (command.ToLower().StartsWith("set")) {
					request.CAS = unique;
				}
				if (!Opcode.AcceptsExtras(request.Opcode)) {
					request.Extras = new byte[0];
				}
				//Write commandline and serialized object.
				socket.Write(request);
				return socket.ReadBinaryResponse();
			});
		}

		protected override object get(string command, string key, bool keyIsChecked, uint hash, out ulong unique)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}
			BinaryRequest req = new BinaryRequest() {
				Opcode = Opcodes.Get,
				KeyAsString = KeyPrefix + key,
			};
			var response = dispatchAndExpectResponse(hash, req);
			unique = 0;
			if (response == null || response.ResponseCode != ErrorCode.None) {
				return null;
			}
			return Deserialize(response, key);
		}

		protected override object[] get(string command, string[] keys, bool keysAreChecked, uint[] hashes, out ulong[] uniques)
		{
			EnsureAlignment(keys, hashes);
			//Check keys.
			if (keys != null && !keysAreChecked) {
				foreach (var key in keys) {
					checkKey(key);
				}
			}

			object[] returnValues = new object[keys.Length];
			uniques = new ulong[keys.Length];
			var responses = executeBulk(
				new BinaryRequest() {Opcode = Opcodes.Get},
				keys,
				hashes,
				null);
			for (int i = 0; i < responses.Length; i++) {
				var response = responses[i];
				if (response == null) {
					continue;
				}
				uniques[i] = response.CAS;
				returnValues[i] = Deserialize(response, keys[i]);
			}
			return returnValues;
		}

		private BinaryResponse[] executeBulk(BinaryRequest template,
			string[] keys, uint[] hashes, object[] values)
		{
			Opcodes quietOpcode = Opcode.ToQuiet(template.Opcode);
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
			BinaryResponse[] returnValues = new BinaryResponse[keys.Length];
			foreach (KeyValuePair<SocketPool, Dictionary<string, List<int>>> kv in dict) {
				Dictionary<UInt32, string> opaqueToKey = new Dictionary<UInt32, string>();
				serverPool.Execute(kv.Key, delegate(PooledSocket socket) {
					int i = 0;
					int total = kv.Value.Count;
					socket.ResetSequence();
					UInt32 sequence = socket.NextSequence;
					UInt32 minSequence = sequence;
					foreach (var value in kv.Value) {
						BinaryRequest req = template.Clone();
						if (i++ < total - 1) {
							req.Opcode = quietOpcode;
						}
						if (values != null) {
							byte[] bytes = new byte[0];
							SerializedType type = SerializedType.Bool;
							var dataIndex = value.Value[value.Value.Count - 1];
							var data = values[dataIndex];
							//Serialize object efficiently, store the datatype marker in the flags property.
							try {
								bytes = Serializer.Serialize(data, out type, CompressionThreshold);
							}
							catch (Exception e) {
								//If serialization fails, return false;

								logger.Error("Error serializing object for key '" + value.Key + "'.", e);
							}
							byte[] extras = new byte[8];
							Array.Copy(BinaryMessage.NetworkOrder(BitConverter.GetBytes((int) type)), 0, extras, 0, 4);
							Array.Copy(BinaryMessage.NetworkOrder(BitConverter.GetBytes((int) 0)), 0, extras, 4, 4); // TODO: handle expiry
							req.Value = bytes;
							req.Extras = extras;
						}
						req.KeyAsString = value.Key;
						req.Opaque = sequence;
						opaqueToKey[sequence] = value.Key;
						sequence = socket.NextSequence;
						socket.Write(req);
					}
					var responses = socket.ReadBinaryResponseBetween(minSequence, sequence - 1);

					//Read values, one by one
					foreach (var response in responses) {
						var gottenKey = opaqueToKey[response.Opaque];
						foreach (int position in kv.Value[gottenKey]) {
							returnValues[position] = response;
						}
					}
				});
			}
			return returnValues;
		}

		protected override bool delete(string key, bool keyIsChecked, uint hash, int time)
		{
			if (!keyIsChecked) {
				checkKey(key);
			}
			BinaryRequest req = new BinaryRequest() {
				Opcode = Opcodes.Delete,
				KeyAsString = KeyPrefix + key,
			};
			var response = dispatchAndExpectResponse(hash, req);
			if (response == null || response.ResponseCode != ErrorCode.None) {
				return false;
			}
			return true;
		}

		protected override ulong? getCounter(string key, bool keyIsChecked, uint hash)
		{
			throw new NotImplementedException();
		}

		protected override ulong?[] getCounter(string[] keys, bool keysAreChecked, uint[] hashes)
		{
			throw new NotImplementedException();
		}

		protected override ulong? incrementDecrement(string cmd, string key, bool keyIsChecked, ulong value, uint hash)
		{
			throw new NotImplementedException();
		}

		public override bool FlushAll(TimeSpan delay, bool staggered)
		{
			throw new NotImplementedException();
		}

		protected override Dictionary<string, string> stats(SocketPool pool)
		{
			throw new NotImplementedException();
		}

		private BinaryResponse dispatchAndExpectResponse(uint hash, BinaryRequest req)
		{
			return serverPool.Execute(hash, null, (socket) => {
				socket.Write(req);
				return socket.ReadBinaryResponse();
			});
		}

		private object Deserialize(BinaryResponse response, String key = null)
		{
			key = key ?? response.KeyAsString;
			SerializedType type = SerializedType.ByteArray;
			if (response.Extras.Length >= 4) {
				type = (SerializedType) BinaryMessage.NetworkUInt16(response.Extras, 2);
			}
			return Deserialize(key, type, response.Value);
		}
	}
}
