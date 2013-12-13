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

		internal static UInt16 NetworkUInt16(byte[] data, int offset)
		{
			if (BitConverter.IsLittleEndian) {
				data = new byte[] {
					data[offset + 1],
					data[offset]
				};
				offset = 0;
			}
			return BitConverter.ToUInt16(data, offset);
		}

		internal static UInt32 NetworkUInt32(byte[] data, int offset)
		{
			if (BitConverter.IsLittleEndian) {
				data = new byte[] {
					data[offset + 3],
					data[offset + 2],
					data[offset + 1],
					data[offset]
				};
				offset = 0;
			}
			return BitConverter.ToUInt32(data, offset);
		}

		internal static UInt64 NetworkUInt64(byte[] data, int offset)
		{
			if (BitConverter.IsLittleEndian) {
				data = new byte[] {
					data[offset + 7],
					data[offset + 6],
					data[offset + 5],
					data[offset + 4],
					data[offset + 3],
					data[offset + 2],
					data[offset + 1],
					data[offset]
				};
				offset = 0;
			}
			return BitConverter.ToUInt64(data, offset);
		}

		internal static byte[] NetworkOrder(byte[] data)
		{
			if (BitConverter.IsLittleEndian) {
				Array.Reverse(data);
			}
			return data;
		}

	}

	internal class BinaryRequest : BinaryMessage
	{
		public const byte Magic = 0x80;

		public void Write(Stream stream)
		{
			List<byte> header = new List<byte>();
			header.Add(Magic);
			header.Add((byte) Opcode);
			header.AddRange(NetworkOrder(BitConverter.GetBytes((UInt16) Key.Length)));
			header.Add((byte) Extras.Length);
			header.AddRange(new byte[] {0x00, 0x00, 0x00}); //data type and two reserved bytes
			header.AddRange(NetworkOrder(BitConverter.GetBytes(Value.Length + Key.Length + Extras.Length)));
			header.AddRange(NetworkOrder(BitConverter.GetBytes(Opaque)));
			header.AddRange(NetworkOrder(BitConverter.GetBytes(CAS)));
			stream.Write(header.ToArray(), 0, header.Count);
			stream.Write(Extras, 0, Extras.Length);
			stream.Write(Key, 0, Key.Length);
			stream.Write(Value, 0, Value.Length);
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
			int keyLength = NetworkUInt16(header, 2);
			int extrasLength = header[4];
			// no need for data type
			ResponseCode = (ErrorCode) NetworkUInt16(header, 6);
			uint bodyLength = NetworkUInt32(header, 8);
			Opaque = NetworkUInt32(header, 12);
			CAS = NetworkUInt64(header, 16);

			uint valueLength = bodyLength - (uint) extrasLength - (uint) keyLength;

			Extras = new byte[extrasLength];
			Key = new byte[keyLength];
			Value = new byte[valueLength];
			if (extrasLength > 0) {
				stream.Read(Extras, 0, extrasLength);
			}
			if (keyLength > 0) {
				stream.Read(Key, 0, keyLength);
			}
			if (valueLength > 0) {
				stream.Read(Value, 0, (int) valueLength);
			}
		}
	}
}
