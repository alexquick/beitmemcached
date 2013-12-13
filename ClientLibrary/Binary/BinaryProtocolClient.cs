using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BeIT.MemCached;

namespace BeITMemcached.ClientLibrary.Binary
{
	internal class BinaryProtocolClient : MemcachedClient
	{
		public BinaryProtocolClient(string name, string[] hosts) : base(name, hosts)
		{
		}

		public override ProtocolType Protocol { get { return ProtocolType.Binary; } }

		protected override bool store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry)
		{
			return store(command, key, keyIsChecked, value, hash, expiry, 0).ResponseCode == ErrorCode.None;
		}

		//Hook for the Append and Prepend commands.
		protected override bool store(string command, string key, bool keyIsChecked, object value, uint hash)
		{
			return store(command, key, keyIsChecked, value, hash, 0, 0).ResponseCode == ErrorCode.None;
		}

		protected override CasResult store(string key, bool keyIsChecked, object value, uint hash, int expiry, ulong unique)
		{
			throw new NotImplementedException();
		}

		private BinaryResponse store(string command, string key, bool keyIsChecked, object value, uint hash, int expiry, ulong unique)
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
				Array.Copy(BinaryMessage.NetworkOrder(BitConverter.GetBytes((int) type)), 0, extras, 0, 4);
				Array.Copy(BinaryMessage.NetworkOrder(BitConverter.GetBytes(expiry)), 0, extras, 4, 4);
				var request = new BinaryRequest() {
					Extras = extras,
					Value = bytes,
					KeyAsString = keyPrefix + key
				};
				//Create commandline
				switch (command) {
					case "set":
						request.Opcode = Opcodes.Set;
						break;
					case "add":
						request.Opcode = Opcodes.Add;
						break;
					case "replace":
						request.Opcode = Opcodes.Replace;
						break;
					case "append":
						extras = new byte[] {};
						request.Opcode = Opcodes.Append;
						break;
					case "prepend":
						extras = new byte[] {};
						request.Opcode = Opcodes.Prepend;
						break;
					case "cas":
						request.Opcode = Opcodes.Set;
						request.CAS = unique;
						break;
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
			SerializedType type = SerializedType.ByteArray;
			if (response.Extras.Length >= 4) {
				type = (SerializedType) BinaryMessage.NetworkUInt16(response.Extras, 2);
			}
			return Deserialize(key, type, response.Value);
		}

		protected override object[] get(string command, string[] keys, bool keysAreChecked, uint[] hashes, out ulong[] uniques)
		{
			throw new NotImplementedException();
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
	}
}
