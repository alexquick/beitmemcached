using System;
using System.Collections.Generic;
using BeIT.MemCached;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Test
{
	[TestFixture]
	internal class BasicTests
	{
		[Test, TestCaseSource("Clients")]
		public void TestBasicGetAndSet(MemcachedClient c)
		{
			Assert.IsNull(c.Get("test"));
			Assert.IsTrue(c.Set("test", "OKAY"));
			Assert.AreEqual("OKAY", c.Get("test"));
		}

		[Test, TestCaseSource("Clients")]
		public void TestBasicDelete(MemcachedClient c)
		{
			Assert.IsNull(c.Get("test"));
			Assert.IsTrue(c.Set("test", "OKAY"));
			Assert.IsNotNull(c.Get("test"));
			Assert.IsTrue(c.Delete("test"));
			Assert.IsNull(c.Get("test"));
		}

		[Test, TestCaseSource("Clients")]
		public void TestBulkGet(MemcachedClient c)
		{
			c.Set("test1", "test1_key");
			c.Set("test3", "test3_key");
			var res = c.Get(new string[] {"test1", "test2", "test3"});
			CollectionAssert.AreEquivalent(new String[] {"test1_key", null, "test3_key"}, res);
		}

		public IEnumerable<TestCaseData> Clients()
		{
			MemcachedClient.Reset();
			MemcachedClient.Setup("default", new string[] {"localhost:11211"});
			MemcachedClient.GetInstance("default").KeyPrefix = Guid.NewGuid().ToString();
			MemcachedClient.SetupBinary("default-binary", new string[] {"localhost:11211"});
			MemcachedClient.GetInstance("default-binary").KeyPrefix = Guid.NewGuid().ToString();

			Assert.AreEqual(MemcachedClient.ProtocolType.Text, MemcachedClient.GetInstance("default").Protocol);
			Assert.AreEqual(MemcachedClient.ProtocolType.Binary, MemcachedClient.GetInstance("default-binary").Protocol);
			return new List<TestCaseData>() {
				new TestCaseData(MemcachedClient.GetInstance("default")).SetName("Text Client"),
				new TestCaseData(MemcachedClient.GetInstance("default-binary")).SetName("Binary Client")
			};
		}
	}
}