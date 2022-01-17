/*
* copyright svante karlsson at csi dot se 2021
*/
#include <gtest/gtest.h>
#include <nonstd/decimal.h>

using decimal52_t = nonstd::decimal<int16_t, 5,2>;
using decimal94_t = nonstd::decimal<int32_t, 9,4>;
using decimal174_t = nonstd::decimal<int64_t, 17,4>;
using decimal188_t = nonstd::decimal<int64_t, 18,8>;
using decimal308_t = nonstd::decimal<int128_t, 30, 8>;

TEST(precision_test, container) {

  double eps = 10.0 * std::numeric_limits<double>::epsilon();
  double d = 31419E-4;
  decimal94_t d94a(d);
  decimal94_t d94b(d);
  EXPECT_EQ(d94a, d94b);
  EXPECT_EQ(std::abs(d - (double)d94a) < eps, true);

  int32_t m = ~0;
  decimal94_t d94 = decimal94_t::make_from_rep(m);
  decimal174_t ans = decimal174_t(d94) * m;
  EXPECT_EQ(ans.get_rep(), (int64_t)m * (int64_t)m);
  ans = -decimal174_t(d94) * -m;
  EXPECT_EQ(ans.get_rep(), (int64_t)(-m) * (int64_t)(-m));
}

// TEST(mult_div_test, container) {
//   {
//     double eps = (double)(decimal94_t::EPSILON * 10);
//     double d = 31419;
//     decimal188_t d94a(d);
//     EXPECT_EQ(abs((double)(d94a * d) - (double)d94a * d) <= eps, true);
//     EXPECT_EQ(abs((double)(d94a / d) - 1.0) <= eps, true);
//   }
//   {
//     double eps = (double)(decimal94_t::EPSILON * 10);
//     double d = 31419E-4;
//     decimal94_t d94a(d);
//     EXPECT_EQ(abs((double)(d94a * d) - (double)d94a * d) <= eps, true);
//     EXPECT_EQ(abs((double)(d94a / d) - 1.0) <= eps, true);
//   }
// }

TEST(min_max_test, container) {
  EXPECT_EQ(decimal94_t::MININT, -1000000000);
  EXPECT_EQ(decimal94_t::MAXINT, 1000000000);
}

TEST(double_cast_test, container) {
  double eps = 10.0 * std::numeric_limits<double>::epsilon();
  decimal94_t d94 = decimal94_t::make_from_rep(31415);
  EXPECT_EQ((abs((double)3.1415 - (double)d94) <= eps), true);

  int16_t c = 314;
  decimal52_t d52_v1 = decimal52_t::make_from_rep(c);
  decimal94_t d94_v1 = decimal94_t::make_from_rep(31400);
  decimal174_t d174_v1 = decimal174_t::make_from_rep(31419L);
  decimal94_t d94_v2(d52_v1);
  decimal174_t d174_v2(d52_v1);

  decimal308_t d308_v1 = decimal308_t::make_from_rep(314000000LL);
  decimal308_t d308_v2(d52_v1);

  EXPECT_EQ((abs((double)3.14 - (double)d94_v2) <= eps), true);
  EXPECT_EQ((abs((double)d94_v2 - (double)3.14) <= eps), true);
  EXPECT_EQ((abs((double)d94_v1 - (double)d52_v1) <= eps), true);
  EXPECT_EQ((abs((double)d94_v2 - (double)d52_v1) <= eps), true);
  EXPECT_EQ((abs((double)d94_v2 - (double)d52_v1) <= eps), true);
  EXPECT_EQ((abs((double)d174_v2 - (double)3.14) <= eps), true);
  EXPECT_EQ((abs((double)d174_v1 - (double)3.1419) <= eps), true);

  EXPECT_EQ((abs((double)d308_v1 - (double)3.14) <= eps), true);
  EXPECT_EQ((abs((double)d308_v2 - (double)3.14) <= eps), true);
}

TEST(decimal_cast_test, container) {
  double eps = 10.0 * std::numeric_limits<double>::epsilon();
  {
    // cast decimal52 to decimal94
    decimal52_t pi = decimal52_t::make_from_rep((int16_t)314);
    decimal94_t pi2 = (decimal94_t)pi;
    EXPECT_EQ((abs((double)3.14 - (double)pi2) <= eps), true);
  }

  {
    // cast decimal94 to decimal308_t
    decimal94_t pi = decimal94_t::make_from_rep(31415);
    decimal308_t pi2 = (decimal308_t)pi;
    EXPECT_EQ((abs((double)3.1415 - (double)pi2) <= eps), true);
  }
}

TEST(int128_test, container) {
  decimal308_t d308_v1 = decimal308_t::make_from_rep(314000000LL);
  d308_v1 = d308_v1 * 10;
}
TEST(dec_multiply_test, container) {
  // multiply decimal94
  {
    decimal94_t pi = decimal94_t(3.14);
    auto x = pi * pi;
    EXPECT_EQ((pi * pi == decimal94_t(9.8596)), true);
    EXPECT_EQ((pi * 2.5 == decimal94_t(7.85)), true);
  }

  // multiply decimal174
  {
    decimal174_t pi = decimal174_t(3.14);
    EXPECT_EQ((pi * pi == decimal174_t(9.8596)), true);
    EXPECT_EQ((pi * 2.5 == decimal174_t(7.85)), true);
  }

  // divide decimal174
  {
    decimal174_t a = decimal174_t(7.85);
    decimal174_t b = a / 2.5;
    EXPECT_EQ((a / 2.5 == decimal174_t(3.14)), true);
  }


  // multiply decimal188
  {
    decimal188_t pi = decimal188_t(3.14);
    EXPECT_EQ((pi * pi == decimal188_t(9.8596)), true);
    EXPECT_EQ((pi * 2.5 == decimal188_t(7.85)), true);
  }
}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
