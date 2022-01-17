/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <any>
#include <functional>
#include <gtest/gtest.h>

#include "qtr/io/parquet_file_sink.h"
#include "qtr/io/parquet_file_source.h"
#include <qtr/parquet/parquet_primitive_row.h>
#include "qtr/schema/parquet_json_schema.h"
#include "qtr/io/csv_file_source.h"

#include "qtr/utils/csv_utils.h"
#include "qtr/utils/parquet_schema_utils.h"

static std::string data_data_root_s = "../tests/test_data";

TEST(parquet_conversion, container) {

  const std::string parquet_v1_filename = "/tmp/test1.1st.parquet";
  const std::string parquet_v2_filename = "/tmp/test1.2st.parquet";

  std::shared_ptr<parquet::schema::GroupNode> parquet_schema_v0 =
      qtr::load_parquet_schema(data_data_root_s +
                               "/test1_v1.parquet-schema.json");
  EXPECT_NE(parquet_schema_v0, nullptr);

  {
    // auto row = qtr::generic_parquet_row::make(parquet_schema_v0.get());
    auto row = qtr::parquet_primitive_row::make_unique(parquet_schema_v0);

    // parquet::schema::Equals
    // parquet::schema::Equals
    //  debugging help
    /*std:: cout << "parquet_schema_v0 : " <<
    pcore::to_string_v1(parquet_schema_v0) << std::endl; std:: cout <<
    "generic_row schema: " << pcore::to_string_v1(row->group_node()) <<
    std::endl;

    std:: cout << "parquet_schema_v0 : " <<
    pcore::to_string_v2(parquet_schema_v0) << std::endl; std:: cout <<
    "generic_row schema: " << pcore::to_string_v2(row->group_node()) <<
    std::endl;
    */

    qtr::verify_equal(parquet_schema_v0, row->group_node());

    EXPECT_EQ(qtr::is_equal(parquet_schema_v0, row->group_node()), true);
  }

  std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>>
      parquet_schema_primitive_nodes_v0;
  for (auto i = 0; i < parquet_schema_v0->field_count(); ++i) {
    parquet_schema_primitive_nodes_v0.emplace_back(
        std::static_pointer_cast<parquet::schema::PrimitiveNode>(
            parquet_schema_v0->field(i)));
  }

  {
    // auto dummy = pcore::generic_parquet_row::make(parquet_schema_v0.get());
    auto dummy = qtr::parquet_primitive_row::make_unique(parquet_schema_v0);

    std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>> columns =
        dummy->primitive_nodes();
    EXPECT_EQ(columns, parquet_schema_primitive_nodes_v0);
  }

  // convert to a parquet file
  {
    qtr::csv_file_source csv_source(data_data_root_s + "/test1.csv.gz");
    auto v = csv_source.column_names();

    EXPECT_EQ(parquet_schema_v0->field_count(),
              v.size()); // same number of columns

    // same column names
    for (size_t i = 0; i != v.size(); ++i) {
      EXPECT_EQ(parquet_schema_v0->field(i)->name(), v.at(i));
    }

    // auto row_p1 = pcore::generic_parquet_row::make(parquet_schema_v0.get());
    auto row_p1 = qtr::parquet_primitive_row::make_unique(parquet_schema_v0);

    auto schema_of_row1 = row_p1->group_node();

    EXPECT_EQ(parquet_schema_v0->field_count(),
              schema_of_row1->field_count()); // still same number of columns

    // still same column names
    for (size_t i = 0; i != v.size(); ++i) {
      EXPECT_EQ(parquet_schema_v0->field(i)->name(), v.at(i));
    }

    // auto sink = pcore::parquet_sink<pcore::generic_parquet_row>(
    //     parquet_v1_filename, parquet_schema_v0);
    //

    auto sink = qtr::parquet_file_sink<qtr::parquet_primitive_row>(
        parquet_v1_filename, parquet_schema_v0);

    std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>>
        primitive_nodes = row_p1->primitive_nodes();

    while (!csv_source.eof()) {
      auto csv_row = csv_source.get_row();
      // auto row = pcore::generic_parquet_row::make(parquet_schema_v0.get());
      auto row = qtr::parquet_primitive_row::make_unique(parquet_schema_v0);
      for (size_t i = 0; i != row->field_count(); ++i) {
        auto v = qtr::csv_utils::parse_from_string(
            primitive_nodes[i], csv_row[i]); // todo move to generic parquet???
        row->assign(i, v);
      }
      sink.push_back(std::move(row));
    }
    // Cleanup
    sink.flush();
  } // parquet file v1 done

  {
    /*auto parquet_source =
        pcore::parquet_file_source<pcore::generic_parquet_row>(
            parquet_v1_filename);
    */

    auto parquet_source = qtr::parquet_file_source<qtr::parquet_primitive_row>(
        parquet_v1_filename);

    auto sink = qtr::parquet_file_sink<qtr::parquet_primitive_row>(
        parquet_v2_filename, parquet_schema_v0);

    auto parquet_schema_v1 = parquet_source.group_node();
    EXPECT_EQ(qtr::is_equal(parquet_schema_v0, parquet_schema_v1), true);

    while (auto src_row = parquet_source.get_next()) {
      auto dst_row = qtr::parquet_primitive_row::make_unique(parquet_schema_v0);
      for (size_t i = 0; i != dst_row->field_count(); ++i) {
        auto src_column = src_row->value(i);
        dst_row->assign(i, src_column);
      }
      sink.push_back(std::move(dst_row));
    }
    sink.flush();
  }

  // check that we still have tsame schema
  {
    auto parquet_source = qtr::parquet_file_source<qtr::parquet_primitive_row>(
        parquet_v2_filename);

    auto parquet_schema_v2 = parquet_source.group_node();
    EXPECT_EQ(qtr::is_equal(parquet_schema_v0, parquet_schema_v2), true);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  if (argc > 1)
    data_data_root_s = argv[1];

  return RUN_ALL_TESTS();
}
