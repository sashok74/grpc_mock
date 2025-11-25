#include "crow.h"
#include "table.grpc.pb.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>

// Universal value used for cells (aligned with protobuf oneof)
using CellValue = std::variant<std::string, int, double, bool, std::nullptr_t>;

enum class ColumnType {
    String,
    Number,
    Currency,
    Bool,
};

std::string toString(ColumnType t) {
    switch (t) {
    case ColumnType::String:
        return "string";
    case ColumnType::Number:
        return "number";
    case ColumnType::Currency:
        return "currency";
    case ColumnType::Bool:
        return "bool";
    }
    return "string";
}

struct ColumnDef {
    std::string id;
    std::string title;
    ColumnType type;
    int width;
    bool is_tree;
    bool is_pinned;
    bool is_editable;
    bool is_primary;
};

struct Row {
    std::string id;                          // primary key value
    std::optional<std::string> parent_id;    // parent key for tree nodes
    std::map<std::string, CellValue> cells;  // dynamic cells keyed by column id
};

struct Table {
    std::string id;
    std::string name;
    std::string primary_key;
    std::string parent_key;
    std::vector<ColumnDef> schema;
    std::vector<Row> rows;
};

crow::json::wvalue valueToJson(const CellValue& v) {
    if (std::holds_alternative<int>(v)) return std::get<int>(v);
    if (std::holds_alternative<double>(v)) return std::get<double>(v);
    if (std::holds_alternative<bool>(v)) return std::get<bool>(v);
    if (std::holds_alternative<std::string>(v)) return std::get<std::string>(v);
    return nullptr;
}

class DataStore {
public:
    DataStore();

    std::vector<std::pair<std::string, std::string>> listTables() const;

    Table* getTable(const std::string& id) {
        auto it = tables_.find(id);
        if (it == tables_.end()) return nullptr;
        return &it->second;
    }

    const Table* getTable(const std::string& id) const {
        auto it = tables_.find(id);
        if (it == tables_.end()) return nullptr;
        return &it->second;
    }

    bool updateCell(const std::string& table_id,
                    const std::string& row_id,
                    const std::string& column_id,
                    const CellValue& value,
                    std::string& error_message);

private:
    std::map<std::string, Table> tables_;
};

DataStore::DataStore() {
    Table employees;
    employees.id = "employees";
    employees.name = "HR";
    employees.primary_key = "id";
    employees.parent_key = "pid";
    employees.schema = {
        {"id", "ID", ColumnType::String, 120, true, true, false, true},
        {"name", "Name", ColumnType::String, 260, true, false, true, false},
        {"position", "Position", ColumnType::String, 200, false, false, true, false},
        {"salary", "Salary", ColumnType::Currency, 120, false, false, true, false},
        {"active", "Active", ColumnType::Bool, 80, false, false, true, false},
    };

    employees.rows.push_back({"1", std::nullopt, {{"id", "1"}, {"name", "Ivanov I.I."}, {"position", "CEO"}, {"salary", 500000}, {"active", true}}});
    employees.rows.push_back({"2", std::make_optional<std::string>("1"), {{"id", "2"}, {"name", "Petrov P.P."}, {"position", "CTO"}, {"salary", 400000}, {"active", true}}});
    employees.rows.push_back({"3", std::make_optional<std::string>("2"), {{"id", "3"}, {"name", "Sidorov S.S."}, {"position", "Senior Engineer"}, {"salary", 300000}, {"active", true}}});
    employees.rows.push_back({"4", std::make_optional<std::string>("2"), {{"id", "4"}, {"name", "Kuznetsov K.K."}, {"position", "Junior Engineer"}, {"salary", 80000}, {"active", false}}});
    employees.rows.push_back({"5", std::nullopt, {{"id", "5"}, {"name", "Accounting"}, {"position", "Department"}, {"salary", 0}, {"active", true}}});
    employees.rows.push_back({"6", std::make_optional<std::string>("5"), {{"id", "6"}, {"name", "Smirnova A.A."}, {"position", "Chief Accountant"}, {"salary", 250000}, {"active", true}}});

    tables_[employees.id] = std::move(employees);

    Table inventory;
    inventory.id = "inventory";
    inventory.name = "Warehouse";
    inventory.primary_key = "sku";
    inventory.parent_key = "parent_sku";
    inventory.schema = {
        {"sku", "SKU", ColumnType::String, 160, true, true, false, true},
        {"item_name", "Item", ColumnType::String, 300, false, false, true, false},
        {"qty", "Quantity", ColumnType::Number, 100, false, false, true, false},
        {"price", "Unit price", ColumnType::Currency, 120, false, false, true, false},
        {"zone", "Zone", ColumnType::String, 80, false, false, true, false},
    };

    inventory.rows.push_back({"100", std::nullopt, {{"sku", "ELEC-001"}, {"item_name", "Electronics"}, {"qty", 0}, {"price", 0.0}, {"zone", "A"}}});
    inventory.rows.push_back({"101", std::make_optional<std::string>("100"), {{"sku", "CPU-INT-9"}, {"item_name", "Intel Core i9"}, {"qty", 45}, {"price", 500.0}, {"zone", "A1"}}});
    inventory.rows.push_back({"102", std::make_optional<std::string>("100"), {{"sku", "GPU-NV-40"}, {"item_name", "Nvidia RTX 4090"}, {"qty", 12}, {"price", 1800.0}, {"zone", "A2"}}});
    inventory.rows.push_back({"200", std::nullopt, {{"sku", "FURN-001"}, {"item_name", "Furniture"}, {"qty", 0}, {"price", 0.0}, {"zone", "B"}}});
    inventory.rows.push_back({"201", std::make_optional<std::string>("200"), {{"sku", "CH-OFF-B"}, {"item_name", "Office Chair"}, {"qty", 150}, {"price", 120.0}, {"zone", "B5"}}});

    tables_[inventory.id] = std::move(inventory);
}

std::vector<std::pair<std::string, std::string>> DataStore::listTables() const {
    std::vector<std::pair<std::string, std::string>> list;
    list.reserve(tables_.size());
    for (const auto& [key, val] : tables_) {
        list.push_back({key, val.name});
    }
    return list;
}

bool DataStore::updateCell(const std::string& table_id,
                           const std::string& row_id,
                           const std::string& column_id,
                           const CellValue& value,
                           std::string& error_message) {
    auto table_it = tables_.find(table_id);
    if (table_it == tables_.end()) {
        error_message = "Table not found";
        return false;
    }

    Table& table = table_it->second;
    auto col_it = std::find_if(table.schema.begin(), table.schema.end(), [&](const ColumnDef& c) {
        return c.id == column_id;
    });

    if (col_it == table.schema.end()) {
        error_message = "Column not found";
        return false;
    }

    if (col_it->is_primary) {
        error_message = "Primary key column is read-only";
        return false;
    }

    if (!col_it->is_editable) {
        error_message = "Column is read-only";
        return false;
    }

    auto row_it = std::find_if(table.rows.begin(), table.rows.end(), [&](const Row& r) {
        return r.id == row_id;
    });

    if (row_it == table.rows.end()) {
        error_message = "Row not found";
        return false;
    }

    row_it->cells[column_id] = value;
    return true;
}

const ColumnDef* findColumn(const Table& table, const std::string& column_id) {
    auto it = std::find_if(table.schema.begin(), table.schema.end(), [&](const ColumnDef& column) {
        return column.id == column_id;
    });
    return it == table.schema.end() ? nullptr : &(*it);
}

std::optional<CellValue> parseJsonValueForColumn(const crow::json::rvalue& node,
                                                 const ColumnDef& column,
                                                 std::string& error_message) {
    using crow::json::type;

    if (node.t() == type::Null) return std::nullptr_t{};

    switch (column.type) {
    case ColumnType::String:
        if (node.t() == type::String) return node.s();
        error_message = "Expected string value";
        return std::nullopt;
    case ColumnType::Bool:
        if (node.t() == type::True || node.t() == type::False) return node.b();
        error_message = "Expected boolean value";
        return std::nullopt;
    case ColumnType::Number:
    case ColumnType::Currency:
        if (node.t() == type::Number) {
            double number = node.d();
            if (column.type == ColumnType::Number && std::floor(number) == number) {
                return static_cast<int>(number);
            }
            return number;
        }
        error_message = "Expected numeric value";
        return std::nullopt;
    }

    error_message = "Unsupported type";
    return std::nullopt;
}

tables::ColumnType toProtoColumnType(ColumnType type) {
    switch (type) {
    case ColumnType::String:
        return tables::COLUMN_TYPE_STRING;
    case ColumnType::Number:
        return tables::COLUMN_TYPE_NUMBER;
    case ColumnType::Currency:
        return tables::COLUMN_TYPE_CURRENCY;
    case ColumnType::Bool:
        return tables::COLUMN_TYPE_BOOL;
    }
    return tables::COLUMN_TYPE_STRING;
}

tables::Value toProtoValue(const CellValue& value) {
    tables::Value proto;
    if (std::holds_alternative<std::string>(value)) {
        proto.set_string_value(std::get<std::string>(value));
    } else if (std::holds_alternative<int>(value)) {
        proto.set_int_value(static_cast<int64_t>(std::get<int>(value)));
    } else if (std::holds_alternative<double>(value)) {
        proto.set_double_value(std::get<double>(value));
    } else if (std::holds_alternative<bool>(value)) {
        proto.set_bool_value(std::get<bool>(value));
    } else {
        proto.set_null_value(::google::protobuf::NullValue::NULL_VALUE);
    }
    return proto;
}

std::optional<CellValue> parseProtoValueForColumn(const tables::Value& proto_value,
                                                  const ColumnDef& column,
                                                  std::string& error_message) {
    switch (proto_value.kind_case()) {
    case tables::Value::kNullValue:
        return std::nullptr_t{};
    case tables::Value::kStringValue:
        if (column.type == ColumnType::String) return proto_value.string_value();
        error_message = "Expected string value";
        return std::nullopt;
    case tables::Value::kBoolValue:
        if (column.type == ColumnType::Bool) return proto_value.bool_value();
        error_message = "Expected boolean value";
        return std::nullopt;
    case tables::Value::kIntValue:
        if (column.type == ColumnType::Currency) return static_cast<double>(proto_value.int_value());
        if (column.type == ColumnType::Number) return static_cast<int>(proto_value.int_value());
        error_message = "Expected numeric value";
        return std::nullopt;
    case tables::Value::kDoubleValue:
        if (column.type == ColumnType::Currency) return proto_value.double_value();
        if (column.type == ColumnType::Number) {
            double number = proto_value.double_value();
            if (std::floor(number) == number) return static_cast<int>(number);
            return number;
        }
        error_message = "Expected numeric value";
        return std::nullopt;
    case tables::Value::KIND_NOT_SET:
    default:
        error_message = "Value is missing";
        return std::nullopt;
    }
}

void fillProtoSchema(const Table& table, tables::TableSchema* schema) {
    schema->set_table_id(table.id);
    schema->set_name(table.name);
    schema->set_primary_key(table.primary_key);
    schema->set_parent_key(table.parent_key);

    for (const auto& column : table.schema) {
        auto* proto_column = schema->add_columns();
        proto_column->set_id(column.id);
        proto_column->set_title(column.title);
        proto_column->set_type(toProtoColumnType(column.type));
        proto_column->set_width(column.width);
        proto_column->set_is_tree(column.is_tree);
        proto_column->set_is_pinned(column.is_pinned);
        proto_column->set_is_editable(column.is_editable && !column.is_primary);
        proto_column->set_is_primary(column.is_primary);
    }
}

class TableServiceImpl final : public tables::TableService::Service {
public:
    explicit TableServiceImpl(DataStore& db)
        : db_(db) {}

    grpc::Status ListTables(grpc::ServerContext*,
                            const tables::ListTablesRequest*,
                            tables::ListTablesResponse* response) override {
        auto list = db_.listTables();
        for (const auto& [id, name] : list) {
            auto* info = response->add_tables();
            info->set_id(id);
            info->set_name(name);
        }
        return grpc::Status::OK;
    }

    grpc::Status GetSchema(grpc::ServerContext*,
                           const tables::GetSchemaRequest* request,
                           tables::GetSchemaResponse* response) override {
        const Table* table = db_.getTable(request->table_id());
        if (!table) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "table not found");
        }

        fillProtoSchema(*table, response->mutable_schema());
        return grpc::Status::OK;
    }

    grpc::Status GetData(grpc::ServerContext*,
                         const tables::GetDataRequest* request,
                         tables::GetDataResponse* response) override {
        const Table* table = db_.getTable(request->table_id());
        if (!table) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "table not found");
        }

        for (const auto& row : table->rows) {
            auto* proto_row = response->add_rows();
            proto_row->set_id(row.id);
            if (row.parent_id.has_value()) proto_row->set_parent_id(row.parent_id.value());

            auto* cells = proto_row->mutable_cells();
            for (const auto& [key, value] : row.cells) {
                (*cells)[key] = toProtoValue(value);
            }
        }

        return grpc::Status::OK;
    }

    grpc::Status UpdateCell(grpc::ServerContext*,
                            const tables::UpdateCellRequest* request,
                            tables::UpdateCellResponse* response) override {
        const Table* table = db_.getTable(request->table_id());
        if (!table) {
            response->set_ok(false);
            response->set_error_message("table not found");
            return grpc::Status::OK;
        }

        const ColumnDef* column = findColumn(*table, request->column_id());
        if (!column) {
            response->set_ok(false);
            response->set_error_message("column not found");
            return grpc::Status::OK;
        }

        std::string parse_error;
        auto value_opt = parseProtoValueForColumn(request->value(), *column, parse_error);
        if (!value_opt.has_value()) {
            response->set_ok(false);
            response->set_error_message(parse_error);
            return grpc::Status::OK;
        }

        std::string update_error;
        if (!db_.updateCell(request->table_id(), request->row_id(), request->column_id(), value_opt.value(), update_error)) {
            response->set_ok(false);
            response->set_error_message(update_error);
            return grpc::Status::OK;
        }

        response->set_ok(true);
        return grpc::Status::OK;
    }

private:
    DataStore& db_;
};

crow::json::wvalue buildSchemaJson(const Table& table) {
    crow::json::wvalue payload;
    payload["tableId"] = table.id;
    payload["name"] = table.name;
    payload["primaryKey"] = table.primary_key;
    payload["parentKey"] = table.parent_key;

    for (std::size_t i = 0; i < table.schema.size(); ++i) {
        const auto& column = table.schema[i];
        payload["columns"][i]["id"] = column.id;
        payload["columns"][i]["title"] = column.title;
        payload["columns"][i]["type"] = toString(column.type);
        payload["columns"][i]["width"] = column.width;
        payload["columns"][i]["isTreeColumn"] = column.is_tree;
        payload["columns"][i]["isPinned"] = column.is_pinned;
        payload["columns"][i]["isEditable"] = column.is_editable && !column.is_primary;
        payload["columns"][i]["isPrimary"] = column.is_primary;
    }

    return payload;
}

crow::json::wvalue buildRowsJson(const Table& table) {
    crow::json::wvalue payload;

    for (std::size_t i = 0; i < table.rows.size(); ++i) {
        const auto& row = table.rows[i];
        payload[i][table.primary_key] = row.id;

        if (!table.parent_key.empty()) {
            if (row.parent_id.has_value()) {
                payload[i][table.parent_key] = row.parent_id.value();
            } else {
                payload[i][table.parent_key] = nullptr;
            }
        }

        for (const auto& [key, value] : row.cells) {
            payload[i][key] = valueToJson(value);
        }
    }

    return payload;
}

int main() {
    DataStore db;

    TableServiceImpl grpc_service(db);
    grpc::ServerBuilder builder;
    const std::string grpc_address = "0.0.0.0:50051";
    builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&grpc_service);

    std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
    if (!grpc_server) {
        std::cerr << "Failed to start gRPC server" << std::endl;
        return 1;
    }
    std::cout << "gRPC server listening on " << grpc_address << std::endl;

    std::jthread grpc_thread([&grpc_server]() {
        grpc_server->Wait();
    });
    (void)grpc_thread;

    crow::SimpleApp app;

    auto setCors = [](crow::response& res) {
        res.add_header("Access-Control-Allow-Origin", "*");
        res.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.add_header("Access-Control-Allow-Headers", "Content-Type");
    };

    CROW_ROUTE(app, "/api/<path>").methods("OPTIONS"_method)([setCors](const crow::request&, crow::response& res, const std::string&) {
        setCors(res);
        res.end();
    });

    CROW_ROUTE(app, "/api/tables")([&db, setCors](const crow::request&, crow::response& res) {
        setCors(res);
        crow::json::wvalue payload;
        auto list = db.listTables();
        for (std::size_t i = 0; i < list.size(); ++i) {
            payload[i]["id"] = list[i].first;
            payload[i]["name"] = list[i].second;
        }
        res.write(payload.dump());
        res.end();
    });

    CROW_ROUTE(app, "/api/table/<string>/schema")([&db, setCors](const crow::request&, crow::response& res, std::string table_id) {
        setCors(res);
        const Table* table = db.getTable(table_id);
        if (!table) {
            res.code = 404;
            res.write(R"({"error":"table not found"})");
            res.end();
            return;
        }

        auto payload = buildSchemaJson(*table);
        res.write(payload.dump());
        res.end();
    });

    CROW_ROUTE(app, "/api/table/<string>/data")([&db, setCors](const crow::request&, crow::response& res, std::string table_id) {
        setCors(res);
        const Table* table = db.getTable(table_id);
        if (!table) {
            res.code = 404;
            res.write(R"({"error":"table not found"})");
            res.end();
            return;
        }

        auto payload = buildRowsJson(*table);
        res.write(payload.dump());
        res.end();
    });

    CROW_ROUTE(app, "/api/table/<string>/update").methods("POST"_method)([&db, setCors](const crow::request& req, crow::response& res, std::string table_id) {
        setCors(res);
        auto body = crow::json::load(req.body);
        if (!body) {
            res.code = 400;
            res.write(R"({"error":"invalid json"})");
            res.end();
            return;
        }

        const Table* table = db.getTable(table_id);
        if (!table) {
            res.code = 404;
            res.write(R"({"error":"table not found"})");
            res.end();
            return;
        }

        if (!body.has("row_id") || !body.has("column_id") || !body.has("value")) {
            res.code = 400;
            res.write(R"({"error":"row_id, column_id and value are required"})");
            res.end();
            return;
        }

        std::string row_id = body["row_id"].s();
        std::string column_id = body["column_id"].s();

        const ColumnDef* column = findColumn(*table, column_id);
        if (!column) {
            res.code = 404;
            res.write(R"({"error":"column not found"})");
            res.end();
            return;
        }

        std::string parse_error;
        auto value_opt = parseJsonValueForColumn(body["value"], *column, parse_error);
        if (!value_opt.has_value()) {
            res.code = 400;
            res.write(std::string("{\"error\":\"") + parse_error + "\"}");
            res.end();
            return;
        }

        std::string update_error;
        if (!db.updateCell(table_id, row_id, column_id, value_opt.value(), update_error)) {
            res.code = 400;
            res.write(std::string("{\"error\":\"") + update_error + "\"}");
            res.end();
            return;
        }

        res.write(R"({"status":"ok"})");
        res.end();
    });

    app.port(8083).multithreaded().run();

    grpc_server->Shutdown();
    return 0;
}
