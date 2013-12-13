using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using BeIT.MemCached;

namespace BeITMemcached.ClientLibrary.Binary
{

		public enum ErrorCode
		{
			None = 0x0000,
			KeyNotFound = 0x0001,
			KeyExists = 0x0002,
			ValueTooLarge = 0x0003,
			InvalidArguments = 0x0004,
			ItemNotStored = 0x0005,
			InvalidIncrTarget = 0x0006,
			UnknownCommand = 0x0081,
			OutOfMemory = 0x0082
		}

	internal class BinaryMessage
		{
		    private static readonly byte[] NoBytes = new byte[0];
            public Opcodes Opcode = Opcodes.Noop;
			public const byte DataType = 0x00;
			public UInt32 Opaque = 0x00;
			public UInt64 CAS = 0x00;

            // payloads
			public byte[] Extras = NoBytes;
			public byte[] Key = NoBytes;
			public byte[] Value = NoBytes;

			public String ExtrasAsString
			{
				get { return Encoding.UTF8.GetString(Extras); }
                set { Extras = Encoding.UTF8.GetBytes(value); }
			}

			public String KeyAsString
			{
				get { return Encoding.UTF8.GetString(Key); }
                set { Key = Encoding.UTF8.GetBytes(value); }
			}

			public String ValueAsString
			{
				get { return Encoding.UTF8.GetString(Value); }
                set { Value = Encoding.UTF8.GetBytes(value); }
			}

		}

		internal class BinaryRequest : BinaryMessage
		{
			public const byte Magic = 0x81;

			public void Write(Stream stream)
			{
				List<byte> header = new List<byte>();
				header.Add(Magic);
				header.Add((byte) Opcode);
				header.AddRange(BitConverter.GetBytes(Key.Length));
				header.AddRange(BitConverter.GetBytes(Extras.Length));
				header.AddRange(new byte[] {0x00, 0x00, 0x00}); //data type and two reserved bytes
				header.AddRange(BitConverter.GetBytes(Value.Length));
				header.AddRange(BitConverter.GetBytes(CAS));
				stream.Write(header.ToArray(), 0, header.Count);
				stream.Write(Extras, 0, Extras.Length);
				stream.Write(Key, 0, Key.Length);
				stream.Write(Value, 0, Extras.Length + Key.Length + Value.Length);
			}
		}


		internal class BinaryResponse : BinaryMessage
		{
			public const byte Magic = 0x81;
			public ErrorCode ResponseCode;
			public void Read(Stream stream)
			{
				byte[] header = new byte[24];
				stream.Read(header, 0, header.Length);
				if (header[0] != Magic) {
					throw new MemcachedClientException("Recieved message that doesn't seem like a response");
				}
				Opcode = (Opcodes) header[1];
				int keyLength = BitConverter.ToUInt16(header, 2);
		        int extrasLength = header[4];
                // no need for data type
				ResponseCode = (ErrorCode) BitConverter.ToUInt16(header, 6);
				uint bodyLength = BitConverter.ToUInt32(header, 8);
				Opaque = BitConverter.ToUInt32(header, 12);
				CAS = BitConverter.ToUInt64(header, 16);

				uint valueLength = bodyLength - (uint)extrasLength -(uint)keyLength;

				Extras = new byte[extrasLength];
                Key = new byte[keyLength];
                Value = new byte[valueLength];
				stream.Read(Extras, 0, extrasLength);
				stream.Read(Key, 0, keyLength);
				stream.Read(Value, 0, (int)valueLength);
			}
		}
}
