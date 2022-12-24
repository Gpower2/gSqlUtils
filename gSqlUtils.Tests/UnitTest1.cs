using System.Collections.Generic;
using gpower2.gSqlUtils;
using Xunit;

namespace gSqlUtils.Tests
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            TestClass t1 = new TestClass
            {
                IntProperty = 1,
                DecimalProperty = 2.3m,
                ListProperty = new List<TestClass>()
            };

            string t1str = ClipboardExtensions.GetClipboardText(t1, System.Globalization.CultureInfo.InvariantCulture, true, "\t");
        }

        class TestClass
        {
            public int IntProperty { get; set; }

            public decimal DecimalProperty { get; set; }

            public List<TestClass> ListProperty { get; set; }
        }
    }
}