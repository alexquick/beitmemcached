using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using BeIT.MemCached;
using NUnit.Framework;

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
			return new List<TestCaseData>() {
				new TestCaseData(MemcachedClient.GetInstance("default")).SetName("Text Client")
			};
		} 
	}
}