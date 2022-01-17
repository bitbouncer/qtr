/**
 * automatic generated file
 */

#include "test1_v1.h"
#include <arrow/type_traits.h>
#include <arrow/builder.h>
#include <qtr/io/stream_writer_ex.h>
#include <qtr/io/stream_reader_ex.h>

static std::shared_ptr<parquet::schema::GroupNode> make_group_node(){
  using namespace parquet;
  using namespace parquet::schema;
  NodeVector nv;
  nv.push_back(make_node(decltype(test1_v1::exchange)(), "exchange"));
  nv.push_back(make_node(decltype(test1_v1::symbol)(), "symbol"));
  nv.push_back(make_node(decltype(test1_v1::timestamp)(), "timestamp"));
  nv.push_back(make_node(decltype(test1_v1::local_timestamp)(), "local_timestamp"));
  nv.push_back(make_node(decltype(test1_v1::type)(), "type"));
  nv.push_back(make_node(decltype(test1_v1::strike_price)(), "strike_price"));
  nv.push_back(make_node(decltype(test1_v1::expiration)(), "expiration"));
  nv.push_back(make_node(decltype(test1_v1::open_interest)(), "open_interest"));
  nv.push_back(make_node(decltype(test1_v1::last_price)(), "last_price"));
  nv.push_back(make_node(decltype(test1_v1::bid_price)(), "bid_price"));
  nv.push_back(make_node(decltype(test1_v1::bid_amount)(), "bid_amount"));
  nv.push_back(make_node(decltype(test1_v1::bid_iv)(), "bid_iv"));
  nv.push_back(make_node(decltype(test1_v1::ask_price)(), "ask_price"));
  nv.push_back(make_node(decltype(test1_v1::ask_amount)(), "ask_amount"));
  nv.push_back(make_node(decltype(test1_v1::ask_iv)(), "ask_iv"));
  nv.push_back(make_node(decltype(test1_v1::mark_price)(), "mark_price"));
  nv.push_back(make_node(decltype(test1_v1::mark_iv)(), "mark_iv"));
  nv.push_back(make_node(decltype(test1_v1::underlying_index)(), "underlying_index"));
  nv.push_back(make_node(decltype(test1_v1::underlying_price)(), "underlying_price"));
  nv.push_back(make_node(decltype(test1_v1::delta)(), "delta"));
  nv.push_back(make_node(decltype(test1_v1::gamma)(), "gamma"));
  nv.push_back(make_node(decltype(test1_v1::vega)(), "vega"));
  nv.push_back(make_node(decltype(test1_v1::theta)(), "theta"));
  nv.push_back(make_node(decltype(test1_v1::rho)(), "rho"));
  return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, nv));
}

std::shared_ptr<parquet::schema::GroupNode> test1_v1::group_node(){
  static std::shared_ptr<parquet::schema::GroupNode> group_node_s = make_group_node();
  return group_node_s;
}

void encode(const test1_v1& obj, parquet::StreamWriter &stream){
  stream << obj.exchange;
  stream << obj.symbol;
  stream << obj.timestamp;
  stream << obj.local_timestamp;
  stream << obj.type;
  stream << obj.strike_price;
  stream << obj.expiration;
  stream << obj.open_interest;
  stream << obj.last_price;
  stream << obj.bid_price;
  stream << obj.bid_amount;
  stream << obj.bid_iv;
  stream << obj.ask_price;
  stream << obj.ask_amount;
  stream << obj.ask_iv;
  stream << obj.mark_price;
  stream << obj.mark_iv;
  stream << obj.underlying_index;
  stream << obj.underlying_price;
  stream << obj.delta;
  stream << obj.gamma;
  stream << obj.vega;
  stream << obj.theta;
  stream << obj.rho;
  stream.EndRow();
}

void decode(test1_v1& obj, parquet::StreamReader &stream){
  stream >> obj.exchange;
  stream >> obj.symbol;
  stream >> obj.timestamp;
  stream >> obj.local_timestamp;
  stream >> obj.type;
  stream >> obj.strike_price;
  stream >> obj.expiration;
  stream >> obj.open_interest;
  stream >> obj.last_price;
  stream >> obj.bid_price;
  stream >> obj.bid_amount;
  stream >> obj.bid_iv;
  stream >> obj.ask_price;
  stream >> obj.ask_amount;
  stream >> obj.ask_iv;
  stream >> obj.mark_price;
  stream >> obj.mark_iv;
  stream >> obj.underlying_index;
  stream >> obj.underlying_price;
  stream >> obj.delta;
  stream >> obj.gamma;
  stream >> obj.vega;
  stream >> obj.theta;
  stream >> obj.rho;
  stream.EndRow();
}


